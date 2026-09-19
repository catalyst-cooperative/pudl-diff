"""CLI for comparing tables between two PUDL Parquet datasets."""

import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import click
from dagster import get_dagster_logger

import pudl
from pudl.logging_helpers import get_logger
from pudl.validate import diff
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

_EPILOG = """
\b
Examples:
\b
  # Local build vs. the last nightly build (the default comparison)
  pudl_diff out_eia__yearly_generators
\b
  # Several specific tables
  pudl_diff out_eia__yearly_generators out_eia__yearly_plants
\b
  # Every table present in both datasets, writing all reports to ./diffs
  pudl_diff --left ~/nightly --right $PUDL_OUTPUT/parquet --output-path diffs
\b
  # Two arbitrary datasets, local or remote
  pudl_diff out_eia__yearly_generators \\
      --left s3://pudl.catalyst.coop/stable --right $PUDL_OUTPUT/parquet
\b
  # A core_ table vs. the out_ table built from it, within the local build
  pudl_diff core_eia860__scd_utilities --right-table out_eia__yearly_utilities \\
      --left $PUDL_OUTPUT/parquet
"""


@dataclass(frozen=True)
class _TableOutcome:
    """What happened when comparing one table, for reporting."""

    table_name: str
    exit_code: int
    """``0`` if identical, ``1`` if different, ``2`` if the comparison failed."""
    report_path: Path
    elapsed_seconds: float | None
    error: str | None
    added: int | None = None
    """Rows only in the right table (for a table with a primary key, rows whose
    primary key is only in the right table). ``None`` unless row-level comparison
    ran."""
    changed: int | None = None
    """Rows whose primary key is in both tables but whose other values changed.
    ``None`` unless row-level comparison ran on a table with a primary key."""
    removed: int | None = None
    """Like :attr:`added`, but for the left table."""
    skipped_reason: str | None = None
    """Why row-level comparison was skipped, if it was."""
    has_primary_key: bool | None = None
    """``None`` if the comparison failed before this was known."""
    left_rows: int | None = None
    right_rows: int | None = None
    left_columns: int | None = None
    columns_added: int | None = None
    """Columns only in the right table. ``None`` if the comparison failed."""
    columns_removed: int | None = None
    """Columns only in the left table."""
    dtypes_changed: int = 0
    """Number of shared columns whose dtype differs between the tables."""
    peak_rss_bytes: int | None = None


@dataclass(frozen=True)
class _RowSummary:
    """The row-level results of a table comparison, in the form we display."""

    added: int | None = None
    changed: int | None = None
    removed: int | None = None
    skipped_reason: str | None = None
    has_primary_key: bool | None = None


def _summarize_row_diff(row_diff: diff.RowDiffSummary | None) -> _RowSummary:
    """Boil a table's row diff report down to what we display."""
    if row_diff is None:
        return _RowSummary()
    pk_diff, non_pk_diff = row_diff.pk_diff, row_diff.non_pk_diff
    if isinstance(pk_diff, diff.PkRowDiffSummary):
        return _RowSummary(
            added=pk_diff.only_in_right_count,
            changed=pk_diff.changed_row_count,
            removed=pk_diff.only_in_left_count,
            has_primary_key=True,
        )
    if isinstance(non_pk_diff, diff.NonPkRowDiffSummary):
        return _RowSummary(
            added=non_pk_diff.only_in_right_count,
            removed=non_pk_diff.only_in_left_count,
            has_primary_key=False,
        )
    # Row-level comparison was skipped. The reason is on whichever section would
    # have run; the other one just says why it doesn't apply.
    not_applicable = {"primary_key_available", "no_primary_key"}
    reasons = [
        section.skipped_reason
        for section in (pk_diff, non_pk_diff)
        if section.skipped_reason not in not_applicable
    ]
    return _RowSummary(
        skipped_reason=reasons[0] if reasons else None,
        has_primary_key=non_pk_diff.skipped_reason == "primary_key_available",
    )


def _diff_table(
    left_dataset: diff.PudlDiffDataset,
    right_dataset: diff.PudlDiffDataset,
    table_name: str,
    *,
    right_table: str | None,
    output_path: Path,
    max_compare_rows: int,
    max_output_rows: int | None,
    rtol: float,
    atol: float,
    partition_expr: str | None,
    auto_partition: bool,
) -> _TableOutcome:
    """Compare one table and write its JSON report and Parquet outputs."""
    run = diff.run_table_diff(
        left_dataset,
        right_dataset,
        table_name,
        right_table_name=right_table,
        partition_expr=partition_expr,
        auto_partition=auto_partition,
        rtol=rtol,
        atol=atol,
        max_rows_for_row_level_comparison=max_compare_rows,
    )

    parquet_outputs = None
    if run.success and run.result is not None:
        parquet_outputs = diff.write_row_diff_parquet(
            run.result.row_diff,
            output_path,
            table_name,
            right_table_name=run.result.right_table_name,
            max_rows_per_output_parquet=max_output_rows,
        )

    report = diff.build_table_diff_report(
        run,
        left_dataset,
        right_dataset,
        table_name,
        right_table_name=right_table,
        parquet_outputs=parquet_outputs,
    )

    output_path.mkdir(parents=True, exist_ok=True)
    report_path = output_path / f"{table_name}_diff.json"
    report_path.write_text(report.model_dump_json(indent=2))

    exit_code = 0 if report.is_identical else 1
    if not run.success:
        exit_code = 2
    rows = _summarize_row_diff(report.row_diff)
    return _TableOutcome(
        table_name=table_name,
        exit_code=exit_code,
        report_path=report_path,
        elapsed_seconds=report.elapsed_seconds,
        error=run.error,
        added=rows.added,
        changed=rows.changed,
        removed=rows.removed,
        skipped_reason=rows.skipped_reason,
        has_primary_key=rows.has_primary_key,
        left_rows=report.row_count_diff.left_row_count
        if report.row_count_diff
        else None,
        right_rows=(
            report.row_count_diff.right_row_count if report.row_count_diff else None
        ),
        left_columns=(
            report.schema_diff.left_column_count if report.schema_diff else None
        ),
        columns_added=(
            len(report.schema_diff.columns_only_in_right)
            if report.schema_diff
            else None
        ),
        columns_removed=(
            len(report.schema_diff.columns_only_in_left) if report.schema_diff else None
        ),
        dtypes_changed=len(report.schema_diff.dtype_changes)
        if report.schema_diff
        else 0,
        peak_rss_bytes=report.peak_rss_bytes,
    )


_GRAY = "bright_black"
_TAGS = {
    0: ("[IDENTICAL]", "green"),
    1: ("[CHANGED]", "yellow"),
    2: ("[ERROR]", "red"),
}
"""The log-level style tag and its color for each :attr:`_TableOutcome.exit_code`."""
_TAG_WIDTH = 11
_KEY_WIDTH = 5
_LEFT_COLUMNS_WIDTH = 9
_COLUMNS_WIDTH = 19
_LEFT_ROWS_WIDTH = 13
_ROWS_WIDTH = 30
_PERCENT_WIDTH = 24
_ELAPSED_WIDTH = 9

_Color = str | int
"""A color name click knows, or an ANSI 256-color code."""
_HOT_PINK = 205
_COLUMN_COLORS = ("cyan", _HOT_PINK, "magenta")
"""Colors for columns added, changed (dtype) and removed."""
_Segments = list[tuple[str, _Color | None]]
"""Pieces of text and the color (if any) to show each one in."""


def _format_elapsed(seconds: float) -> str:
    """Format a duration with enough precision to be meaningful for fast tables."""
    return f"{seconds:.3f}s"


def _format_duration(seconds: float) -> str:
    """Format the duration of a whole run, which may be minutes or hours long."""
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02.0f}s"
    return f"{minutes}m {secs:04.1f}s"


def _format_bytes(num_bytes: int) -> str:
    """Format a memory size in decimal units, e.g. ``21.4 GB``."""
    if num_bytes >= 1_000_000_000:
        return f"{num_bytes / 1_000_000_000:.1f} GB"
    return f"{num_bytes / 1_000_000:.1f} MB"


def _format_percent(count: int, total: int | None) -> str:
    """``count`` as a percentage of ``total``, with useful precision when small."""
    if count == 0:
        return "0%"
    if not total:
        return "n/a"
    percent = 100 * count / total
    if percent < 0.01:
        return "<0.01%"
    if percent >= 100:
        return f"{percent:,.0f}%"
    return f"{percent:.2f}%"


def _tables(n: int) -> str:
    return f"{n:,} table{'' if n == 1 else 's'}"


def _render(segments: _Segments, width: int = 0) -> str:
    """Style ``segments``, padding to ``width`` based on their unstyled length."""
    plain = "".join(text for text, _ in segments)
    styled = "".join(
        click.style(text, fg=color) if color else text for text, color in segments
    )
    return styled + " " * max(0, width - len(plain))


def _count(text: str, value: int, color: _Color) -> tuple[str, _Color]:
    """A count in ``color``, or gray if it's zero."""
    return text, color if value else _GRAY


def _change_segments(
    added: int,
    changed: int | None,
    removed: int,
    text: Callable[[int], str],
    colors: tuple[_Color, _Color, _Color] = ("green", "yellow", "red"),
) -> _Segments:
    """Git diff style ``+added/changed/-removed``, shown as ``text(count)``.

    For rows, ``added`` are rows only in the right table (green), ``changed`` are
    rows whose primary key is in both tables but whose values changed (yellow,
    and only for tables with a primary key), and ``removed`` are rows only in
    the left table (red). Zero counts are gray.
    """
    added_color, changed_color, removed_color = colors
    separator = ("/", _GRAY)
    segments = [_count(f"+{text(added)}", added, added_color), separator]
    if changed is not None:
        segments += [_count(text(changed), changed, changed_color), separator]
    return [*segments, _count(f"-{text(removed)}", removed, removed_color)]


def _columns_segments(outcome: _TableOutcome) -> _Segments:
    """Summary of column changes: ``+added/changed/-removed``.

    Here ``changed`` counts the columns whose dtype changed. Cyan, hot pink and
    magenta, to tell them apart from the row counts.
    """
    if outcome.columns_added is None or outcome.columns_removed is None:
        return []
    return _change_segments(
        outcome.columns_added,
        outcome.dtypes_changed,
        outcome.columns_removed,
        lambda n: f"{n:,}",
        colors=_COLUMN_COLORS,
    )


def _row_cells(outcome: _TableOutcome) -> tuple[str, str]:
    """The styled, padded row changes and row change percentage cells.

    If there are no row changes to show, the explanation spans both cells.
    """
    message_width = _ROWS_WIDTH + 2 + _PERCENT_WIDTH
    if outcome.exit_code == 2:
        return _render([("comparison failed", None)], message_width), ""
    if outcome.skipped_reason is not None:
        reason = outcome.skipped_reason.replace("_", " ")
        return _render([(f"row diff skipped: {reason}", None)], message_width), ""
    if outcome.added is None or outcome.removed is None:
        return _render([("no row-level results", None)], message_width), ""
    counts = _change_segments(
        outcome.added, outcome.changed, outcome.removed, lambda n: f"{n:,}"
    )
    percents = _change_segments(
        outcome.added,
        outcome.changed,
        outcome.removed,
        lambda n: _format_percent(n, outcome.left_rows),
    )
    return _render(counts, _ROWS_WIDTH), _render(percents, _PERCENT_WIDTH)


def _format_key(has_primary_key: bool | None) -> str:
    if has_primary_key is None:
        return ""
    return "PK" if has_primary_key else "no-PK"


def _format_header(progress_width: int = 0) -> str:
    """The column headings for the lines made by :func:`_format_outcome`."""
    parts = [
        " " * progress_width,
        "STATUS".ljust(_TAG_WIDTH),
        "KEY".ljust(_KEY_WIDTH),
        "LEFT COLS".rjust(_LEFT_COLUMNS_WIDTH),
        "COLS +add/~chg/-del".ljust(_COLUMNS_WIDTH),
        "LEFT ROWS".rjust(_LEFT_ROWS_WIDTH),
        "ROWS +add/~chg/-del".ljust(_ROWS_WIDTH),
        "% OF LEFT ROWS".ljust(_PERCENT_WIDTH),
        "TIME".rjust(_ELAPSED_WIDTH),
        "TABLE",
    ]
    return click.style("  ".join(part for part in parts if part), bold=True)


def _format_outcome(outcome: _TableOutcome, progress: str = "") -> str:
    """One line summarizing a table's comparison.

    The table name goes last, so that the (variable length) names don't disturb
    the alignment of everything before it.
    """
    tag, color = _TAGS[outcome.exit_code]
    elapsed = (
        _format_elapsed(outcome.elapsed_seconds)
        if outcome.elapsed_seconds is not None
        else ""
    )
    left_rows = f"{outcome.left_rows:,}" if outcome.left_rows is not None else ""
    left_columns = (
        f"{outcome.left_columns:,}" if outcome.left_columns is not None else ""
    )
    row_counts, row_percents = _row_cells(outcome)
    parts = [
        progress,
        click.style(tag.ljust(_TAG_WIDTH), fg=color),
        _format_key(outcome.has_primary_key).ljust(_KEY_WIDTH),
        left_columns.rjust(_LEFT_COLUMNS_WIDTH),
        _render(_columns_segments(outcome), _COLUMNS_WIDTH),
        left_rows.rjust(_LEFT_ROWS_WIDTH),
        row_counts,
        row_percents,
        elapsed.rjust(_ELAPSED_WIDTH),
        outcome.table_name,
    ]
    return "  ".join(part for part in parts if part)


_LABEL_WIDTH = 17


def _field(label: str, value: str) -> str:
    """A summary line: a bold ``label``, then ``value`` aligned with the others."""
    padding = " " * max(2, _LABEL_WIDTH - len(label))
    return f"{click.style(label, bold=True)}{padding}{value}"


def _echo_totals(outcomes: list[_TableOutcome]) -> None:
    """Print row totals and changes summed over all the compared tables."""
    left_total = sum(o.left_rows or 0 for o in outcomes)
    right_total = sum(o.right_rows or 0 for o in outcomes)
    compared = [o for o in outcomes if o.added is not None and o.removed is not None]
    added = sum(o.added or 0 for o in compared)
    changed = sum(o.changed or 0 for o in compared)
    removed = sum(o.removed or 0 for o in compared)
    click.echo(
        _field(
            "Total rows:",
            f"{left_total:,} left, {right_total:,} right ({_tables(len(outcomes))})",
        )
    )
    counts = _change_segments(added, changed, removed, lambda n: f"{n:,}")
    click.echo(_field("Row changes:", _render(counts)))
    percents = _change_segments(
        added, changed, removed, lambda n: _format_percent(n, left_total)
    )
    click.echo(_field("% of left rows:", _render(percents)))
    # Rows in tables we couldn't count changes for still count towards the total
    # above, so say how many there are to make the percentages interpretable.
    uncompared = [o for o in outcomes if o not in compared]
    if uncompared:
        rows = sum(o.left_rows or 0 for o in uncompared)
        click.echo(
            _field(
                "Not compared:",
                f"{_tables(len(uncompared))} ({rows:,} left rows) "
                "had no row-level comparison",
            )
        )


def _echo_schema_totals(outcomes: list[_TableOutcome]) -> None:
    """Print columns added, changed (dtype) and removed across all the tables.

    Followed by each table whose schema changed, and how its columns changed.
    """
    with_schema = [o for o in outcomes if o.columns_added is not None]
    added = sum(o.columns_added or 0 for o in with_schema)
    changed = sum(o.dtypes_changed for o in with_schema)
    removed = sum(o.columns_removed or 0 for o in with_schema)
    changed_tables = [
        o
        for o in with_schema
        if o.columns_added or o.columns_removed or o.dtypes_changed
    ]
    columns = _change_segments(
        added, changed, removed, lambda n: f"{n:,}", colors=_COLUMN_COLORS
    )
    click.echo(_field("Column changes:", _render(columns)))
    click.echo(_field("Schema changes:", _tables(len(changed_tables))))
    # Schema changes can be disruptive to users, so name every table that has one,
    # with its own column changes, one per line.
    width = max((len(o.table_name) for o in changed_tables), default=0)
    for outcome in changed_tables:
        counts = _render(_columns_segments(outcome))
        click.echo(f"  {outcome.table_name.ljust(width)}  {counts}")


def _echo_table_list(label: str, table_names: list[str]) -> None:
    """Print a bold heading and a count, then the tables one per line."""
    if table_names:
        click.echo(f"{click.style(label + ':', bold=True)} {len(table_names):,}")
        for table_name in table_names:
            click.echo(f"  {table_name}")


def _echo_summary(
    outcomes: list[_TableOutcome],
    only_in_left: list[str],
    only_in_right: list[str],
    *,
    left_root: str,
    right_root: str,
    output_path: Path,
    elapsed_seconds: float,
) -> None:
    """Print how the run went: table counts, what was compared, time and memory."""

    def names(exit_code: int) -> list[str]:
        return [o.table_name for o in outcomes if o.exit_code == exit_code]

    errored = names(2)
    counts = [
        ("Identical", len(names(0)), "green"),
        ("Changed", len(names(1)), "yellow"),
        ("Error", len(errored), "red"),
    ]
    click.echo(
        "\n"
        + "  ".join(
            f"{click.style(label, fg=color)}: {n}" for label, n, color in counts
        )
    )
    click.echo(_field("Left:", left_root))
    click.echo(_field("Right:", right_root))
    click.echo(_field("Elapsed:", _format_duration(elapsed_seconds)))
    measured = [o for o in outcomes if o.peak_rss_bytes is not None]
    if measured:
        peak = max(measured, key=lambda o: o.peak_rss_bytes or 0)
        click.echo(
            _field(
                "Peak memory:",
                f"{_format_bytes(peak.peak_rss_bytes or 0)} ({peak.table_name})",
            )
        )
    _echo_totals(outcomes)
    _echo_schema_totals(outcomes)
    _echo_table_list("Tables with errors", errored)
    _echo_table_list("Tables removed (only in left)", only_in_left)
    _echo_table_list("Tables added (only in right)", only_in_right)
    click.echo(f"{click.style('Reports written to', bold=True)} {output_path}")


def _set_log_level(level: str) -> Callable[[], None]:
    """Only let PUDL's loggers emit messages of at least ``level``.

    Returns a function that puts the loggers' levels back as they were, so that
    running the CLI (e.g. in tests) doesn't leave logging reconfigured.
    """
    numeric_level = logging.getLevelName(level.upper())
    saved: list[tuple[logging.Handler | logging.Logger, int]] = []
    for logger in (
        logging.getLogger("catalystcoop"),
        get_dagster_logger("catalystcoop"),
    ):
        for target in (logger, *logger.handlers):
            saved.append((target, target.level))
            target.setLevel(numeric_level)

    def restore() -> None:
        for target, previous_level in saved:
            target.setLevel(previous_level)

    return restore


@click.command(context_settings={"help_option_names": ["-h", "--help"]}, epilog=_EPILOG)
@click.argument("table_names", type=str, nargs=-1)
@click.option(
    "-l",
    "--left",
    type=str,
    default=None,
    help="Root path of the 'left' dataset (local path or URL, e.g. s3://...). "
    "Defaults to PUDL's nightly build outputs on S3, the reference point most "
    "diffs are measured against.",
)
@click.option(
    "-r",
    "--right",
    type=str,
    default=None,
    help="Root path of the 'right' dataset (local path or URL). Defaults to "
    "$PUDL_OUTPUT/parquet, so the diff reads as what's changed locally since "
    "the last nightly build.",
)
@click.option(
    "--right-table",
    type=str,
    default=None,
    help="Name of the table to compare in the right dataset, if it differs "
    "from the table name given - e.g. comparing a core_* table against the "
    "out_* table built from it. Defaults to the same name. Requires exactly "
    "one TABLE_NAME.",
)
@click.option(
    "-o",
    "--output-path",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory to write the JSON report and Parquet outputs into. "
    "Created if it doesn't exist. Defaults to the current working directory.",
)
@click.option(
    "--max-compare-rows",
    type=int,
    default=diff.MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
    show_default=True,
    help="Skip row-level comparison, keeping the cheaper schema and "
    "row-count comparisons, whenever either table has more rows than this.",
)
@click.option(
    "--max-output-rows",
    type=int,
    default=None,
    help="Cap the number of rows written to each Parquet side-output file. "
    "Defaults to writing every differing row.",
)
@click.option(
    "--rtol",
    type=float,
    default=1e-5,
    show_default=True,
    help="Relative tolerance for float equality, matching numpy.isclose.",
)
@click.option(
    "--atol",
    type=float,
    default=1e-8,
    show_default=True,
    help="Absolute tolerance for float equality, matching numpy.isclose.",
)
@click.option(
    "--partition-expr",
    type=str,
    default=None,
    help="Column to group row counts by when comparing them. Defaults to "
    "PUDL's dbt row-count test configuration for this table, if any (see "
    "--no-auto-partition). Applies to every table given, so requires at least "
    "one TABLE_NAME.",
)
@click.option(
    "--no-auto-partition",
    is_flag=True,
    default=False,
    help="Compare whole-table row counts even if a dbt partition is "
    "configured for this table. Ignored if --partition-expr is given.",
)
@click.option(
    "--color/--no-color",
    default=None,
    help="Colorize the output. Defaults to on if stdout is a terminal, and off "
    "otherwise (e.g. when piped to a file).",
)
@click.option(
    "--loglevel",
    default="ERROR",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    show_default=True,
    help="Only show log messages at least this severe, so that they don't "
    "interrupt the report. Skipped comparisons and errors are still recorded in "
    "each table's JSON report.",
)
@click.pass_context
def main(
    ctx: click.Context,
    table_names: tuple[str, ...],
    left: str | None,
    right: str | None,
    right_table: str | None,
    output_path: Path | None,
    max_compare_rows: int,
    max_output_rows: int | None,
    rtol: float,
    atol: float,
    partition_expr: str | None,
    no_auto_partition: bool,
    color: bool | None,
    loglevel: str,
) -> None:
    """Compare tables between two PUDL Parquet datasets.

    Compares each of the given TABLE_NAMES. If none are given, compares every
    table that has a Parquet file in both datasets. Tables are compared one at a
    time in a single process (so PUDL is only imported once). When comparing
    more than one, prints a summary at the end, which for all tables also lists
    those present in only one dataset (they aren't compared).

    Writes a JSON report (and, for differing tables, a pair of Parquet
    side-output files holding the differing rows) for each table to
    --output-path, then exits 0 if all the tables are functionally identical,
    1 if any differ, or 2 if any comparison itself failed (e.g. a table doesn't
    exist in one of the datasets, or a dataset's datapackage.json couldn't be
    read). A table that fails doesn't stop the others being compared.
    """
    if right_table is not None and len(table_names) != 1:
        raise click.UsageError("--right-table requires exactly one TABLE_NAME.")
    if partition_expr is not None and not table_names:
        raise click.UsageError("--partition-expr requires at least one TABLE_NAME.")

    start = time.perf_counter()
    ctx.call_on_close(_set_log_level(loglevel))
    # click.echo consults the context's color setting, so this covers all output.
    ctx.color = sys.stdout.isatty() if color is None else color
    left_root = left or str(pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH)
    right_root = right or str(PudlPaths().parquet_path())
    output_path = output_path or Path.cwd()

    left_dataset = diff.PudlDiffDataset(left_root)
    right_dataset = diff.PudlDiffDataset(right_root)
    diff_options = {
        "output_path": output_path,
        "max_compare_rows": max_compare_rows,
        "max_output_rows": max_output_rows,
        "rtol": rtol,
        "atol": atol,
        "auto_partition": not no_auto_partition,
    }

    if len(table_names) == 1:
        (table_name,) = table_names
        outcome = _diff_table(
            left_dataset,
            right_dataset,
            table_name,
            right_table=right_table,
            partition_expr=partition_expr,
            **diff_options,
        )
        click.echo(
            f"Comparing {table_name!r} between {left_root!r} and {right_root!r}."
        )
        click.echo(_format_header())
        click.echo(_format_outcome(outcome))
        if outcome.error is not None:
            click.echo(
                f"Comparison of {table_name!r} failed: {outcome.error}", err=True
            )
        click.echo(f"Report written to {outcome.report_path}")
        ctx.exit(outcome.exit_code)

    only_in_left: list[str] = []
    only_in_right: list[str] = []
    if table_names:
        tables = list(dict.fromkeys(table_names))
        click.echo(
            f"Comparing {len(tables)} tables between {left_root!r} and {right_root!r}."
        )
    else:
        left_tables = left_dataset.parquet_table_names()
        right_tables = right_dataset.parquet_table_names()
        tables = sorted(set(left_tables) & set(right_tables))
        only_in_left = sorted(set(left_tables) - set(right_tables))
        only_in_right = sorted(set(right_tables) - set(left_tables))
        if not tables:
            click.echo(
                f"No tables found in both {left_root!r} ({len(left_tables)} tables) "
                f"and {right_root!r} ({len(right_tables)} tables).",
                err=True,
            )
            ctx.exit(2)
        click.echo(
            f"Comparing {len(tables)} tables present in both {left_root!r} and "
            f"{right_root!r}."
        )

    outcomes = []
    progress_width = len(f"[{len(tables)}/{len(tables)}]")
    click.echo(_format_header(progress_width))
    for i, table in enumerate(tables, start=1):
        outcome = _diff_table(
            left_dataset,
            right_dataset,
            table,
            right_table=None,
            partition_expr=partition_expr,
            **diff_options,
        )
        outcomes.append(outcome)
        width = len(str(len(tables)))
        click.echo(_format_outcome(outcome, f"[{i:>{width}}/{len(tables)}]"))

    _echo_summary(
        outcomes,
        only_in_left,
        only_in_right,
        left_root=left_root,
        right_root=right_root,
        output_path=output_path,
        elapsed_seconds=time.perf_counter() - start,
    )
    ctx.exit(max(o.exit_code for o in outcomes))


if __name__ == "__main__":
    sys.exit(main())
