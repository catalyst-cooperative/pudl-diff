"""CLI for comparing tables between two PUDL Parquet datasets."""

import logging
import sys
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import click
from dagster import get_dagster_logger

import pudl
from pudl.logging_helpers import get_logger
from pudl.validate.diff import table_report as diff
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import (
    PudlDiffReport,
    PudlDiffSummary,
    build_pudl_diff_report,
)
from pudl.validate.diff.table import MAX_ROWS_FOR_ROW_LEVEL_COMPARISON
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

REPORT_FILENAME = "pudl_diff_report.json"
"""Name of the JSON report, written to the output directory."""

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
    """What happened when comparing one table, for display."""

    table_name: str
    exit_code: int
    """``0`` if identical, ``1`` if different, ``2`` if the comparison failed."""
    elapsed_seconds: float | None
    error: str | None
    rows: diff.RowChanges
    sizes: diff.SizeComparison
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


def _outcome(table_name: str, report: diff.TableDiffReport) -> _TableOutcome:
    """Boil a table's report down to what we display."""
    exit_code = 0 if report.is_identical else 1
    if not report.success:
        exit_code = 2
    row_counts = report.row_count_diff
    schema = report.schema_diff
    return _TableOutcome(
        table_name=table_name,
        exit_code=exit_code,
        elapsed_seconds=report.elapsed_seconds,
        error=report.error,
        rows=diff.RowChanges.from_summary(report.row_diff),
        sizes=report,
        left_rows=row_counts.left_row_count if row_counts else None,
        right_rows=row_counts.right_row_count if row_counts else None,
        left_columns=schema.left_column_count if schema else None,
        columns_added=len(schema.columns_only_in_right) if schema else None,
        columns_removed=len(schema.columns_only_in_left) if schema else None,
        dtypes_changed=len(schema.dtype_changes) if schema else 0,
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
_SIZE_WIDTH = 9
_RIGHT_SIZE_WIDTH = 10
_SIZE_CHANGE_WIDTH = 11
_PERCENT_CHANGE_WIDTH = 9

_Color = str | int
"""A color name click knows, or an ANSI 256-color code."""
_HOT_PINK = 205
_COLUMN_COLORS = ("cyan", _HOT_PINK, "magenta")
"""Colors for columns added, changed (dtype) and removed."""
_GREW = 39
_SHRANK = 208
"""Blue for a table that grew and orange for one that shrank: colors that, unlike
green and red, don't suggest that either change is good or bad."""
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


def _format_signed_percent(percent: float) -> str:
    """A percentage change, always signed, with useful precision when small."""
    if percent == 0:
        return "0%"
    if abs(percent) < 0.01:
        return f"{'+' if percent > 0 else '-'}<0.01%"
    return f"{percent:+,.2f}%"


def _tables(n: int) -> str:
    return f"{n:,} table{'' if n == 1 else 's'}"


def _render(segments: _Segments, width: int = 0) -> str:
    """Style ``segments``, padding to ``width`` based on their unstyled length."""
    plain = "".join(text for text, _ in segments)
    styled = "".join(
        click.style(text, fg=color) if color else text for text, color in segments
    )
    return styled + " " * max(0, width - len(plain))


def _render_right(segments: _Segments, width: int) -> str:
    """Style ``segments``, right-aligned in ``width`` based on their unstyled length."""
    plain = "".join(text for text, _ in segments)
    return " " * max(0, width - len(plain)) + _render(segments)


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


def _size_change_segments(sizes: diff.SizeComparison) -> tuple[_Segments, _Segments]:
    """The size change and its percentage of the left size, colored by direction.

    Blank if either size is unknown.
    """
    if sizes.bytes_difference is None or sizes.bytes_difference_size is None:
        return [], []
    if sizes.bytes_difference > 0:
        color: _Color = _GREW
    else:
        color = _SHRANK if sizes.bytes_difference < 0 else _GRAY
    percent = sizes.bytes_difference_percent
    return (
        [(sizes.bytes_difference_size, color)],
        [(_format_signed_percent(percent), color)] if percent is not None else [],
    )


def _row_cells(outcome: _TableOutcome) -> tuple[str, str]:
    """The styled, padded row changes and row change percentage cells.

    If there are no row changes to show, the explanation spans both cells.
    """
    message_width = _ROWS_WIDTH + 2 + _PERCENT_WIDTH
    if outcome.exit_code == 2:
        return _render([("comparison failed", None)], message_width), ""
    rows = outcome.rows
    if rows.skipped_reason is not None:
        reason = rows.skipped_reason.replace("_", " ")
        return _render([(f"row diff skipped: {reason}", None)], message_width), ""
    if rows.added is None or rows.removed is None:
        return _render([("no row-level results", None)], message_width), ""
    counts = _change_segments(
        rows.added, rows.changed, rows.removed, lambda n: f"{n:,}"
    )
    percents = _change_segments(
        rows.added,
        rows.changed,
        rows.removed,
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
        "LEFT SIZE".rjust(_SIZE_WIDTH),
        "RIGHT SIZE".rjust(_RIGHT_SIZE_WIDTH),
        "SIZE CHANGE".rjust(_SIZE_CHANGE_WIDTH),
        "% SIZE".rjust(_PERCENT_CHANGE_WIDTH),
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
    size_change, size_percent = _size_change_segments(outcome.sizes)
    parts = [
        progress,
        click.style(tag.ljust(_TAG_WIDTH), fg=color),
        _format_key(outcome.rows.has_primary_key).ljust(_KEY_WIDTH),
        left_columns.rjust(_LEFT_COLUMNS_WIDTH),
        _render(_columns_segments(outcome), _COLUMNS_WIDTH),
        left_rows.rjust(_LEFT_ROWS_WIDTH),
        row_counts,
        row_percents,
        (outcome.sizes.left_table_size or "").rjust(_SIZE_WIDTH),
        (outcome.sizes.right_table_size or "").rjust(_RIGHT_SIZE_WIDTH),
        _render_right(size_change, _SIZE_CHANGE_WIDTH),
        _render_right(size_percent, _PERCENT_CHANGE_WIDTH),
        elapsed.rjust(_ELAPSED_WIDTH),
        outcome.table_name,
    ]
    return "  ".join(part for part in parts if part)


_LABEL_WIDTH = 17


def _field(label: str, value: str) -> str:
    """A summary line: a bold ``label``, then ``value`` aligned with the others."""
    padding = " " * max(2, _LABEL_WIDTH - len(label))
    return f"{click.style(label, bold=True)}{padding}{value}"


def _echo_totals(summary: PudlDiffSummary) -> None:
    """Print row and size totals and changes summed over all the compared tables."""
    click.echo(
        _field(
            "Total rows:",
            f"{summary.left_row_count:,} left, {summary.right_row_count:,} right "
            f"({_tables(summary.table_count)})",
        )
    )
    counts = _change_segments(
        summary.rows_added,
        summary.rows_changed,
        summary.rows_removed,
        lambda n: f"{n:,}",
    )
    click.echo(_field("Row changes:", _render(counts)))
    percents = _change_segments(
        summary.rows_added,
        summary.rows_changed,
        summary.rows_removed,
        lambda n: _format_percent(n, summary.left_row_count),
    )
    click.echo(_field("% of left rows:", _render(percents)))
    # Rows in tables we couldn't count changes for still count towards the total
    # above, so say how many there are to make the percentages interpretable.
    if summary.no_row_diff_table_count:
        click.echo(
            _field(
                "Not compared:",
                f"{_tables(summary.no_row_diff_table_count)} "
                f"({summary.no_row_diff_left_row_count:,} left rows) "
                "had no row-level comparison",
            )
        )
    if summary.left_table_size and summary.right_table_size:
        click.echo(
            _field(
                "Total size:",
                f"{summary.left_table_size} left, {summary.right_table_size} right",
            )
        )
        change, percent = _size_change_segments(summary)
        click.echo(_field("Size change:", _render([*change, (" ", None), *percent])))


def _echo_schema_totals(
    summary: PudlDiffSummary, outcomes: list[_TableOutcome]
) -> None:
    """Print columns added, changed (dtype) and removed across all the tables.

    Followed by each table whose schema changed, and how its columns changed.
    """
    columns = _change_segments(
        summary.columns_added,
        summary.columns_changed,
        summary.columns_removed,
        lambda n: f"{n:,}",
        colors=_COLUMN_COLORS,
    )
    click.echo(_field("Column changes:", _render(columns)))
    click.echo(_field("Schema changes:", _tables(len(summary.schema_changed_tables))))
    # Schema changes can be disruptive to users, so name every table that has one,
    # with its own column changes, one per line.
    by_name = {o.table_name: o for o in outcomes}
    width = max((len(name) for name in summary.schema_changed_tables), default=0)
    for name in summary.schema_changed_tables:
        counts = _render(_columns_segments(by_name[name]))
        click.echo(f"  {name.ljust(width)}  {counts}")


def _echo_table_list(label: str, table_names: list[str]) -> None:
    """Print a bold heading and a count, then the tables one per line."""
    if table_names:
        click.echo(f"{click.style(label + ':', bold=True)} {len(table_names):,}")
        for table_name in table_names:
            click.echo(f"  {table_name}")


def _echo_summary(
    report: PudlDiffReport, outcomes: list[_TableOutcome], report_path: Path
) -> None:
    """Print how the run went: table counts, what was compared, time and memory."""
    summary = report.summary
    counts = [
        ("Identical", summary.identical_table_count, "green"),
        ("Changed", summary.changed_table_count, "yellow"),
        ("Error", summary.failed_table_count, "red"),
    ]
    click.echo(
        "\n"
        + "  ".join(
            f"{click.style(label, fg=color)}: {n}" for label, n, color in counts
        )
    )
    click.echo(_field("Left:", report.left_dataset.root))
    click.echo(_field("Right:", report.right_dataset.root))
    if report.elapsed_seconds is not None:
        click.echo(_field("Elapsed:", _format_duration(report.elapsed_seconds)))
    if summary.peak_rss is not None:
        click.echo(
            _field("Peak memory:", f"{summary.peak_rss} ({summary.peak_rss_table})")
        )
    _echo_totals(summary)
    _echo_schema_totals(summary, outcomes)
    _echo_table_list("Tables with errors", summary.failed_tables)
    _echo_table_list("Tables removed (only in left)", report.tables_only_in_left)
    _echo_table_list("Tables added (only in right)", report.tables_only_in_right)
    click.echo(f"{click.style('Report written to', bold=True)} {report_path}")


class _NoTablesError(Exception):
    """There are no tables to compare."""


def _resolve_tables(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_names: tuple[str, ...],
) -> tuple[list[str], list[str], list[str]]:
    """Decide which tables to compare.

    Returns:
        The tables to compare: those given, or else every table with a Parquet
        file in both datasets. Then the tables found only in the left dataset and
        those found only in the right dataset, both empty when tables are given.

    Raises:
        _NoTablesError: If no tables were given, and the datasets have none in
            common.
    """
    if table_names:
        return list(dict.fromkeys(table_names)), [], []
    left_tables = left.parquet_table_names()
    right_tables = right.parquet_table_names()
    tables = sorted(set(left_tables) & set(right_tables))
    if not tables:
        raise _NoTablesError(
            f"No tables found in both {str(left.root)!r} ({len(left_tables)} tables) "
            f"and {str(right.root)!r} ({len(right_tables)} tables)."
        )
    return (
        tables,
        sorted(set(left_tables) - set(right_tables)),
        sorted(set(right_tables) - set(left_tables)),
    )


def _echo_intro(
    tables: list[str], left_root: str, right_root: str, *, explicit: bool
) -> None:
    """Say which tables are about to be compared, and between what."""
    if not explicit:
        click.echo(
            f"Comparing {len(tables)} tables present in both {left_root!r} and "
            f"{right_root!r}."
        )
        return
    what = repr(tables[0]) if len(tables) == 1 else f"{len(tables)} tables"
    click.echo(f"Comparing {what} between {left_root!r} and {right_root!r}.")


def _compare_tables(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    tables: list[str],
    output_path: Path,
    *,
    right_table: str | None,
    options: diff.DiffOptions,
    show_progress: bool,
) -> tuple[dict[str, diff.TableDiffReport], list[_TableOutcome]]:
    """Compare each table in turn, printing a line about each as it's done."""
    reports: dict[str, diff.TableDiffReport] = {}
    outcomes: list[_TableOutcome] = []
    total = len(tables)
    width = len(str(total))
    click.echo(_format_header(len(f"[{total}/{total}]") if show_progress else 0))
    for i, table in enumerate(tables, start=1):
        reports[table] = diff.report_table_diff(
            left,
            right,
            table,
            output_path,
            right_table_name=right_table,
            options=options,
        )
        outcomes.append(_outcome(table, reports[table]))
        progress = f"[{i:>{width}}/{total}]" if show_progress else ""
        click.echo(_format_outcome(outcomes[-1], progress))
    return reports, outcomes


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
    help="Directory to write the JSON report (pudl_diff_report.json) and Parquet "
    "outputs into. "
    "Created if it doesn't exist. Defaults to the current working directory.",
)
@click.option(
    "--max-compare-rows",
    type=int,
    default=MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
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
    "the JSON report.",
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

    Writes a single JSON report covering every table (and, for differing tables,
    a pair of Parquet side-output files holding the differing rows) to
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
    report_path = output_path / REPORT_FILENAME

    left_dataset = PudlDiffDataset(left_root)
    right_dataset = PudlDiffDataset(right_root)
    options = diff.DiffOptions(
        rtol=rtol,
        atol=atol,
        max_compare_rows=max_compare_rows,
        max_output_rows=max_output_rows,
        auto_partition=not no_auto_partition,
        partition_expr=partition_expr,
    )

    def write_report(report: PudlDiffReport) -> None:
        output_path.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report.model_dump_json(indent=2))

    try:
        tables, only_in_left, only_in_right = _resolve_tables(
            left_dataset, right_dataset, table_names
        )
    except _NoTablesError as e:
        tables, only_in_left, only_in_right = [], [], []
        error: str | None = str(e)
    except Exception:
        logger.exception("Couldn't list the tables to compare.")
        tables, only_in_left, only_in_right = [], [], []
        error = traceback.format_exc()
    else:
        error = None
    if error is not None:
        # There's nothing to compare, but the report still records why.
        write_report(
            build_pudl_diff_report(
                left_dataset,
                right_dataset,
                {},
                options=options,
                elapsed_seconds=time.perf_counter() - start,
                error=error,
            )
        )
        click.echo(f"Comparison failed: {error}", err=True)
        click.echo(f"Report written to {report_path}")
        ctx.exit(2)

    single = len(table_names) == 1
    _echo_intro(tables, left_root, right_root, explicit=bool(table_names))
    table_reports, outcomes = _compare_tables(
        left_dataset,
        right_dataset,
        tables,
        output_path,
        right_table=right_table,
        options=options,
        show_progress=not single,
    )

    report = build_pudl_diff_report(
        left_dataset,
        right_dataset,
        table_reports,
        options=options,
        tables_only_in_left=only_in_left,
        tables_only_in_right=only_in_right,
        elapsed_seconds=time.perf_counter() - start,
    )
    write_report(report)
    if single:
        if outcomes[0].error is not None:
            click.echo(
                f"Comparison of {tables[0]!r} failed: {outcomes[0].error}", err=True
            )
        click.echo(f"Report written to {report_path}")
    else:
        _echo_summary(report, outcomes, report_path)
    ctx.exit(report.exit_code)


if __name__ == "__main__":
    sys.exit(main())
