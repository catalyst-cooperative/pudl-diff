"""CLI for comparing tables between two PUDL Parquet datasets."""

import sys
from dataclasses import dataclass
from pathlib import Path

import click

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
    return _TableOutcome(
        table_name=table_name,
        exit_code=exit_code,
        report_path=report_path,
        elapsed_seconds=report.elapsed_seconds,
        error=run.error,
    )


_STATUS = {0: "identical", 1: "DIFFERENT", 2: "FAILED"}


def _echo_summary(
    outcomes: list[_TableOutcome],
    only_in_left: list[str],
    only_in_right: list[str],
    output_path: Path,
) -> None:
    """Print how many of the compared tables were identical, different or failed."""

    def _names(exit_code: int) -> list[str]:
        return [o.table_name for o in outcomes if o.exit_code == exit_code]

    identical, different, failed = _names(0), _names(1), _names(2)
    click.echo(
        f"\nIdentical: {len(identical)}  Different: {len(different)}  "
        f"Failed: {len(failed)}"
    )
    for label, names in [
        ("Different", different),
        ("Failed", failed),
        ("Only in left", only_in_left),
        ("Only in right", only_in_right),
    ]:
        if names:
            click.echo(f"{label}: {' '.join(names)}")
    click.echo(f"Reports written to {output_path}")


@click.command(context_settings={"help_option_names": ["-h", "--help"]}, epilog=_EPILOG)
@click.argument("table_names", type=str, nargs=-1)
@click.option(
    "--left",
    type=str,
    default=None,
    help="Root path of the 'left' dataset (local path or URL, e.g. s3://...). "
    "Defaults to PUDL's nightly build outputs on S3, the reference point most "
    "diffs are measured against.",
)
@click.option(
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
        if outcome.error is not None:
            click.echo(
                f"Comparison of {table_name!r} failed: {outcome.error}", err=True
            )
        else:
            click.echo(
                f"{table_name!r} is {_STATUS[outcome.exit_code]} "
                f"between {left_root!r} and {right_root!r}."
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
        elapsed = (
            f" ({outcome.elapsed_seconds:.1f}s)"
            if outcome.elapsed_seconds is not None
            else ""
        )
        click.echo(
            f"[{i}/{len(tables)}] {table}: {_STATUS[outcome.exit_code]}{elapsed}"
        )

    _echo_summary(outcomes, only_in_left, only_in_right, output_path)
    ctx.exit(max(o.exit_code for o in outcomes))


if __name__ == "__main__":
    sys.exit(main())
