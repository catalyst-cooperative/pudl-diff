"""CLI for comparing a single table between two PUDL Parquet datasets."""

import sys
from pathlib import Path

import click

import pudl
from pudl.logging_helpers import get_logger
from pudl.validate import diff
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("table_name", type=str)
@click.option(
    "--left",
    type=str,
    default=None,
    help="Root path of the 'left' dataset (local path or URL, e.g. s3://...). "
    "Defaults to $PUDL_OUTPUT/parquet.",
)
@click.option(
    "--right",
    type=str,
    default=None,
    help="Root path of the 'right' dataset (local path or URL). Defaults to "
    "PUDL's nightly build outputs on S3.",
)
@click.option(
    "--right-table",
    type=str,
    default=None,
    help="Name of the table to compare in the right dataset, if it differs "
    "from TABLE_NAME - e.g. comparing a core_* table against the out_* "
    "table built from it. Defaults to TABLE_NAME.",
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
    "--partition-col",
    type=str,
    default=None,
    help="Column to group row counts by when comparing them. Defaults to "
    "PUDL's dbt row-count test configuration for this table, if any (see "
    "--no-auto-partition).",
)
@click.option(
    "--no-auto-partition",
    is_flag=True,
    default=False,
    help="Compare whole-table row counts even if a dbt partition is "
    "configured for this table. Ignored if --partition-col is given.",
)
@click.pass_context
def main(
    ctx: click.Context,
    table_name: str,
    left: str | None,
    right: str | None,
    right_table: str | None,
    output_path: Path | None,
    max_compare_rows: int,
    max_output_rows: int | None,
    rtol: float,
    atol: float,
    partition_col: str | None,
    no_auto_partition: bool,
) -> None:
    """Compare TABLE_NAME between two PUDL Parquet datasets.

    Writes a JSON report (and, for differing tables, a pair of Parquet
    side-output files holding the differing rows) to --output-path, then
    exits 0 if the tables are functionally identical, 1 if they differ, or 2
    if the comparison itself failed (e.g. TABLE_NAME doesn't exist in one of
    the datasets, or a dataset's datapackage.json couldn't be read).
    """
    left_root = left or str(PudlPaths().parquet_path())
    right_root = right or str(pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH)
    output_path = output_path or Path.cwd()

    left_dataset = diff.PudlDiffDataset(left_root)
    right_dataset = diff.PudlDiffDataset(right_root)

    run = diff.run_table_diff(
        left_dataset,
        right_dataset,
        table_name,
        right_table_name=right_table,
        partition_col=partition_col,
        auto_partition=not no_auto_partition,
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

    if not run.success:
        click.echo(f"Comparison of {table_name!r} failed: {run.error}", err=True)
        click.echo(f"Report written to {report_path}")
        ctx.exit(2)

    click.echo(
        f"{table_name!r} is "
        f"{'identical' if report.is_identical else 'DIFFERENT'} "
        f"between {left_root!r} and {right_root!r}."
    )
    click.echo(f"Report written to {report_path}")
    ctx.exit(0 if report.is_identical else 1)


if __name__ == "__main__":
    sys.exit(main())
