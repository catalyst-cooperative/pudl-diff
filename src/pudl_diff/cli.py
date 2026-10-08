"""CLI for comparing tables between two PUDL Parquet datasets."""

import logging
import sys
from collections.abc import Callable
from pathlib import Path

import click
from upath import UPath

from pudl_diff import __version__, table_report
from pudl_diff.dataset import PudlDiffDataset
from pudl_diff.dataset_report import REPORT_FILENAME, PudlDiffReport
from pudl_diff.defaults import default_right_root, nightly_root
from pudl_diff.logs import LOGGER_NAME
from pudl_diff.runner import run_dataset_diff
from pudl_diff.table import MAX_COMPARE_ROWS
from pudl_diff.terminal import TerminalProgress, echo_summary

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
    report_path: Path | UPath,
    *,
    single: bool,
    saved: bool,
) -> None:
    """Say how a comparison, or a saved report on one, turned out."""
    verb = "written to" if saved else "read from"
    progress.finish()
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


def _show_saved_report(location: str, *, verbose: bool) -> int:
    """Print a saved report as a comparison would have; return its exit code.

    `location` is a local or remote (e.g. `gs://...`) path to a report file, or to a
    directory containing one.
    """
    report_path = UPath(location)
    try:
        if report_path.is_dir():
            report_path /= REPORT_FILENAME
        report = PudlDiffReport.model_validate_json(
            report_path.read_text(encoding="utf-8")
        )
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
        verbose=verbose,
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
    """Only let the tool's loggers emit messages of at least `level`.

    If the application hasn't configured any handlers for them, as when the tool is
    installed without PUDL, logs go to stderr. Returns a function that puts the
    loggers back as they were, so that running the CLI (e.g. in tests) doesn't leave
    logging reconfigured.
    """
    numeric_level = logging.getLevelNamesMapping()[level.upper()]
    logger = logging.getLogger(LOGGER_NAME)
    added: logging.Handler | None = None
    if not logger.handlers:
        added = logging.StreamHandler()
        added.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)8s] %(name)s: %(message)s")
        )
        logger.addHandler(added)
    saved = [(target, target.level) for target in (logger, *logger.handlers)]
    for target, _ in saved:
        target.setLevel(numeric_level)

    def restore() -> None:
        for target, previous_level in saved:
            target.setLevel(previous_level)
        if added is not None:
            logger.removeHandler(added)

    return restore


@click.command(context_settings={"help_option_names": ["-h", "--help"]}, epilog=_EPILOG)
@click.version_option(__version__, "--version", prog_name="pudl_diff")
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
    type=click.IntRange(min=0),
    default=MAX_COMPARE_ROWS,
    show_default=True,
    help="Skip row-level comparison, keeping the cheaper schema and "
    "row-count comparisons, whenever either table has more rows than this. "
    "The largest tables take minutes to compare, hence the default; raise it, "
    "or give 0 for no limit, to compare them too.",
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
    "--from-report",
    type=str,
    default=None,
    help="Show a saved JSON report (or the pudl_diff_report.json in a directory), "
    "at a local path or a remote URL such as gs://... or s3://..., "
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
    "--verbose/--quiet",
    "verbose",
    default=False,
    help="With --quiet (the default), the live table only lists tables that aren't "
    "identical, and leaves out the size columns. --verbose lists every table, "
    "with sizes. The JSON report and final summary are the same either way.",
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
    from_report: str | None,
    color: bool | None,
    verbose: bool,
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
        ctx.exit(_show_saved_report(from_report, verbose=verbose))
    if right_table is not None and len(table_names) != 1:
        raise click.UsageError("--right-table requires exactly one TABLE_NAME.")

    ctx.call_on_close(_set_log_level(loglevel))
    # click.echo consults the context's color setting, so this covers all output.
    ctx.color = sys.stdout.isatty() if color is None else color
    try:
        left_root = left or str(nightly_root())
        right_root = right or str(default_right_root())
    except RuntimeError as e:
        raise click.UsageError(str(e)) from e
    output_path = output_path or Path.cwd()
    report_path = output_path / REPORT_FILENAME

    left_dataset = PudlDiffDataset(left_root)
    right_dataset = PudlDiffDataset(right_root)
    options = table_report.DiffOptions(
        rtol=rtol,
        atol=atol,
        max_compare_rows=max_compare_rows or None,
        max_output_rows=max_output_rows,
    )

    def write_report(report: PudlDiffReport) -> None:
        output_path.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    single = len(table_names) == 1
    progress = TerminalProgress(
        left_root,
        right_root,
        explicit=bool(table_names),
        show_progress=not single,
        verbose=verbose,
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
