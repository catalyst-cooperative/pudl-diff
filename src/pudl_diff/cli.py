"""CLI for comparing tables between two PUDL Parquet datasets."""

import logging
import sys
from collections.abc import Callable
from pathlib import Path

import click
from dagster import get_dagster_logger

import pudl
from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import REPORT_FILENAME, PudlDiffReport
from pudl.validate.diff.runner import run_dataset_diff
from pudl.validate.diff.table import MAX_ROWS_FOR_ROW_LEVEL_COMPARISON
from pudl.validate.diff.terminal import TerminalProgress, echo_summary
from pudl.workspace.setup import PudlPaths

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
\b
  # Show a report you already have, without comparing anything again
  pudl_diff --from-report path/to/pudl_diff_report.json
"""


def _echo_outcome(
    report: PudlDiffReport,
    progress: TerminalProgress,
    report_path: Path,
    *,
    single: bool,
    saved: bool,
) -> None:
    """Say how a comparison, or a saved report on one, turned out."""
    verb = "written to" if saved else "read from"
    if report.error is not None:
        click.echo(f"Comparison failed: {report.error}", err=True)
        click.echo(f"Report {verb} {report_path}")
    elif single:
        outcome = progress.outcomes[0]
        if outcome.error is not None:
            click.echo(
                f"Comparison of {outcome.table_name!r} failed: {outcome.error}",
                err=True,
            )
        click.echo(f"Report {verb} {report_path}")
    else:
        echo_summary(report, progress.outcomes, report_path, saved=saved)


_COMPARISON_OPTIONS = (
    "table_names",
    "left",
    "right",
    "right_table",
    "output_path",
    "max_compare_rows",
    "max_output_rows",
    "rtol",
    "atol",
    "partition_expr",
    "no_auto_partition",
)


def _reject_comparison_options(ctx: click.Context) -> None:
    """Refuse --from-report alongside anything that only controls a comparison."""
    given = [
        param.opts[-1] if param.opts else param.name
        for param in ctx.command.params
        if param.name in _COMPARISON_OPTIONS
        and ctx.get_parameter_source(param.name)
        not in (None, click.core.ParameterSource.DEFAULT)
    ]
    if given:
        raise click.UsageError(
            f"--from-report doesn't compare anything, so it can't be used with: "
            f"{', '.join(map(str, given))}."
        )


def _show_saved_report(path: Path) -> int:
    """Print a saved report as a comparison would have; return its exit code."""
    report_path = path / REPORT_FILENAME if path.is_dir() else path
    try:
        report = PudlDiffReport.model_validate_json(report_path.read_text())
    except (OSError, ValueError) as e:
        raise click.ClickException(
            f"Couldn't read a report from {report_path}: {e}"
        ) from e
    tables = list(report.tables)
    single = len(tables) == 1
    progress = TerminalProgress(
        report.left_dataset.root,
        report.right_dataset.root,
        explicit=True,
        show_progress=not single,
        intro=(
            f"Report of {len(tables)} tables between {report.left_dataset.root!r} "
            f"and {report.right_dataset.root!r}, created {report.created}."
        ),
    )
    if tables:
        progress.tables_resolved(tables)
        for name, table in report.tables.items():
            progress.table_compared(name, table)
    _echo_outcome(report, progress, report_path, single=single, saved=False)
    return report.exit_code


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
    "--from-report",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Show a saved JSON report (or the pudl_diff_report.json in a directory) "
    "as the tables and summary a new comparison would print, without comparing "
    "anything. Can't be combined with the options that control a comparison.",
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
    from_report: Path | None,
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

    With --from-report, instead shows an existing report in the same form, and
    exits with the code that comparison did.
    """
    if from_report is not None:
        _reject_comparison_options(ctx)
        ctx.call_on_close(_set_log_level(loglevel))
        ctx.color = sys.stdout.isatty() if color is None else color
        ctx.exit(_show_saved_report(from_report))
    if right_table is not None and len(table_names) != 1:
        raise click.UsageError("--right-table requires exactly one TABLE_NAME.")
    if partition_expr is not None and not table_names:
        raise click.UsageError("--partition-expr requires at least one TABLE_NAME.")

    ctx.call_on_close(_set_log_level(loglevel))
    # click.echo consults the context's color setting, so this covers all output.
    ctx.color = sys.stdout.isatty() if color is None else color
    left_root = left or str(pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH)
    right_root = right or str(PudlPaths().parquet_path())
    output_path = output_path or Path.cwd()
    report_path = output_path / REPORT_FILENAME

    left_dataset = PudlDiffDataset(left_root)
    right_dataset = PudlDiffDataset(right_root)
    options = table_report.DiffOptions(
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

    single = len(table_names) == 1
    progress = TerminalProgress(
        left_root, right_root, explicit=bool(table_names), show_progress=not single
    )
    report = run_dataset_diff(
        left_dataset,
        right_dataset,
        output_path,
        table_names=table_names,
        right_table=right_table,
        options=options,
        on_tables_resolved=progress.tables_resolved,
        on_table_compared=progress.table_compared,
    )
    write_report(report)
    _echo_outcome(report, progress, report_path, single=single, saved=True)
    ctx.exit(report.exit_code)


if __name__ == "__main__":
    sys.exit(main())
