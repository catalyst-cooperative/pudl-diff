"""Comparing many tables between two datasets."""

import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path

import pudl.logging_helpers
from pudl.validate.diff.dataset import NoTablesError, PudlDiffDataset, resolve_tables
from pudl.validate.diff.dataset_report import PudlDiffReport, build_pudl_diff_report
from pudl.validate.diff.table_report import (
    DiffOptions,
    TableDiffReport,
    report_table_diff,
)

logger = pudl.logging_helpers.get_logger(__name__)


def run_dataset_diff(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    output_path: Path,
    *,
    table_names: Sequence[str] = (),
    right_table: str | None = None,
    options: DiffOptions | None = None,
    on_tables_resolved: Callable[[list[str]], None] | None = None,
    on_table_compared: Callable[[str, TableDiffReport], None] | None = None,
) -> PudlDiffReport:
    """Compare tables between two datasets, one at a time, and report on them all.

    A table whose comparison fails doesn't stop the others being compared, and if
    the tables to compare can't even be determined, the returned report records why
    in its :attr:`PudlDiffReport.error` instead of raising.

    Args:
        left: The "left" dataset.
        right: The "right" dataset, compared against it.
        output_path: Directory to write each differing table's Parquet outputs into.
        table_names: The tables to compare. If empty, every table with a Parquet
            file in both datasets is compared.
        right_table: The name of the table to compare against in ``right``, if it
            differs from the one given in ``table_names``. Requires exactly one
            table name.
        options: The settings to compare with.
        on_tables_resolved: Called once with the names of the tables that are about
            to be compared, before any of them is.
        on_table_compared: Called with each table's name and report as soon as it's
            been compared.
    """
    start = time.perf_counter()
    options = options or DiffOptions()

    def failed(error: str) -> PudlDiffReport:
        # There's nothing to compare, but the report still records why.
        return build_pudl_diff_report(
            left,
            right,
            {},
            options=options,
            elapsed_seconds=time.perf_counter() - start,
            error=error,
        )

    try:
        tables, only_in_left, only_in_right = resolve_tables(left, right, table_names)
    except NoTablesError as e:
        return failed(str(e))
    except Exception:
        logger.exception("Couldn't list the tables to compare.")
        return failed(traceback.format_exc())

    if on_tables_resolved is not None:
        on_tables_resolved(tables)
    reports: dict[str, TableDiffReport] = {}
    for table in tables:
        reports[table] = report_table_diff(
            left,
            right,
            table,
            output_path,
            right_table_name=right_table,
            options=options,
        )
        if on_table_compared is not None:
            on_table_compared(table, reports[table])
    return build_pudl_diff_report(
        left,
        right,
        reports,
        options=options,
        tables_only_in_left=only_in_left,
        tables_only_in_right=only_in_right,
        elapsed_seconds=time.perf_counter() - start,
    )
