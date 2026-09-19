"""CLI for comparing tables between two PUDL Parquet datasets."""

import logging
import sys
import time
import traceback
from collections.abc import Callable
from pathlib import Path

import click
from dagster import get_dagster_logger

import pudl
from pudl.logging_helpers import get_logger
from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import (
    PudlDiffReport,
    _outcome,
    _TableOutcome,
    build_pudl_diff_report,
)
from pudl.validate.diff.table import MAX_ROWS_FOR_ROW_LEVEL_COMPARISON
from pudl.validate.diff.terminal import (
    _echo_intro,
    _echo_summary,
    _format_header,
    _format_outcome,
)
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


def _compare_tables(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    tables: list[str],
    output_path: Path,
    *,
    right_table: str | None,
    options: table_report.DiffOptions,
    show_progress: bool,
) -> tuple[dict[str, table_report.TableDiffReport], list[_TableOutcome]]:
    """Compare each table in turn, printing a line about each as it's done."""
    reports: dict[str, table_report.TableDiffReport] = {}
    outcomes: list[_TableOutcome] = []
    total = len(tables)
    width = len(str(total))
    click.echo(_format_header(len(f"[{total}/{total}]") if show_progress else 0))
    for i, table in enumerate(tables, start=1):
        reports[table] = table_report.report_table_diff(
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
