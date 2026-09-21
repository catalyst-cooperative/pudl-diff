"""The dataset-level report, summarizing all the tables compared."""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import pydantic

from pudl_diff.base import ReportModel
from pudl_diff.dataset import DatasetProvenance, PudlDiffDataset
from pudl_diff.formatting import format_bytes
from pudl_diff.table_report import (
    DiffOptions,
    RowChanges,
    SizeComparison,
    TableDiffReport,
)

#: Version of the JSON report format written by :func:`build_pudl_diff_report`.
REPORT_SCHEMA_VERSION = "1.0.0"


REPORT_FILENAME = "pudl_diff_report.json"
"""Name of the JSON report, written to the output directory."""


class DatasetInfo(DatasetProvenance):
    """One of the two compared datasets: where it is, and where it came from."""

    root: str
    """The root path or URL of the dataset's Parquet files. For a dataset on the
    local filesystem, an absolute path with any symlinks resolved, so it doesn't
    depend on the directory the comparison was run from."""


class PudlDiffSummary(SizeComparison):
    """Totals over every table in a :class:`PudlDiffReport`.

    Saves consumers from aggregating the tables themselves.

    The size fields (see :class:`~.SizeComparison`) total only the tables whose size
    is known on both sides.
    """

    table_count: int
    """Number of tables compared, including any whose comparison failed."""
    identical_table_count: int
    """Tables whose comparison completed and found no differences."""
    changed_table_count: int
    """Tables whose comparison completed and found differences."""
    failed_table_count: int
    """Tables whose comparison failed to complete."""
    failed_tables: list[str]
    """The names of the tables whose comparison failed."""
    schema_changed_tables: list[str]
    """Tables with columns added or removed, or with changed dtypes."""

    left_row_count: int
    """Total rows in the left tables, over all tables that could be counted."""
    right_row_count: int
    """Total rows in the right tables, over all tables that could be counted."""
    rows_added: int
    """Rows only in the right table, summed over tables with a row-level
    comparison."""
    rows_changed: int
    """Rows with the same primary key but changed values, summed over tables
    with a row-level comparison and a primary key."""
    rows_removed: int
    """Rows only in the left table, summed over tables with a row-level
    comparison."""
    no_row_diff_table_count: int
    """Tables with no row-level comparison, whether skipped or failed. Their rows
    count towards the row totals, but not the rows added, changed or removed."""
    no_row_diff_left_row_count: int
    """Total rows in the left side of those tables."""

    columns_added: int
    """Columns only in the right table, summed over all the tables."""
    columns_changed: int
    """Shared columns whose dtype changed, summed over all the tables."""
    columns_removed: int
    """Columns only in the left table, summed over all the tables."""

    peak_rss_bytes: int | None = None
    """The highest :attr:`~.TableDiffReport.peak_rss_bytes` of any table."""
    peak_rss_table: str | None = None
    """The table with that peak memory use."""

    @pydantic.computed_field
    @property
    def peak_rss(self) -> str | None:
        """:attr:`peak_rss_bytes` in human-readable form, e.g. ``1.2 GB``."""
        if self.peak_rss_bytes is None:
            return None
        return format_bytes(self.peak_rss_bytes)

    @classmethod
    def from_tables(cls, tables: dict[str, TableDiffReport]) -> PudlDiffSummary:
        """Total up the reports of individual tables, keyed by table name."""
        reports = list(tables.values())
        failed = [name for name, report in tables.items() if not report.success]
        changed = [
            name
            for name, report in tables.items()
            if report.success and not report.is_identical
        ]

        row_counts = {
            name: r.row_count_diff
            for name, r in tables.items()
            if r.row_count_diff is not None
        }
        counted = list(row_counts.values())
        changes = {
            name: RowChanges.from_summary(r.row_diff) for name, r in tables.items()
        }
        compared = [
            c for c in changes.values() if c.added is not None and c.removed is not None
        ]
        no_row_diff = [
            name for name, c in changes.items() if c.added is None or c.removed is None
        ]

        schemas = [r.schema_diff for r in reports if r.schema_diff is not None]
        sized = [
            r
            for r in reports
            if r.left_table_bytes is not None and r.right_table_bytes is not None
        ]
        measured = {
            name: report.peak_rss_bytes
            for name, report in tables.items()
            if report.peak_rss_bytes is not None
        }
        peak_table = max(measured, key=measured.__getitem__) if measured else None

        return cls(
            table_count=len(tables),
            identical_table_count=len(tables) - len(failed) - len(changed),
            changed_table_count=len(changed),
            failed_table_count=len(failed),
            failed_tables=failed,
            schema_changed_tables=[
                name
                for name, report in tables.items()
                if report.schema_diff is not None
                and not report.schema_diff.is_identical
            ],
            left_row_count=sum(c.left_row_count for c in counted),
            right_row_count=sum(c.right_row_count for c in counted),
            rows_added=sum(c.added or 0 for c in compared),
            rows_changed=sum(c.changed or 0 for c in compared),
            rows_removed=sum(c.removed or 0 for c in compared),
            no_row_diff_table_count=len(no_row_diff),
            no_row_diff_left_row_count=sum(
                row_counts[name].left_row_count
                for name in no_row_diff
                if name in row_counts
            ),
            columns_added=sum(len(s.columns_only_in_right) for s in schemas),
            columns_changed=sum(len(s.dtype_changes) for s in schemas),
            columns_removed=sum(len(s.columns_only_in_left) for s in schemas),
            left_table_bytes=(
                sum(r.left_table_bytes or 0 for r in sized) if sized else None
            ),
            right_table_bytes=(
                sum(r.right_table_bytes or 0 for r in sized) if sized else None
            ),
            peak_rss_bytes=measured[peak_table] if peak_table is not None else None,
            peak_rss_table=peak_table,
        )


class PudlDiffReport(ReportModel):
    """The full comparison of two PUDL datasets: the saved JSON report.

    Built by :func:`build_pudl_diff_report`. Holds everything that pertains to the
    comparison as a whole, plus a :class:`~.TableDiffReport` for each table.
    """

    schema_version: str = REPORT_SCHEMA_VERSION
    """Version of this report format, in ``major.minor.patch`` form."""
    created: str
    """UTC ISO-8601 timestamp of when this report was generated."""
    elapsed_seconds: float | None = None
    """Wall-clock time the whole comparison took."""
    left_dataset: DatasetInfo
    """The reference dataset, e.g. the last nightly build. Additions, removals and
    changes are all measured from it to the right dataset."""
    right_dataset: DatasetInfo
    """The dataset that was compared against the left one, e.g. a local build."""
    options: DiffOptions
    """The settings the comparison was run with."""
    tables_only_in_left: list[str]
    """Tables found only in the left dataset, which aren't compared."""
    tables_only_in_right: list[str]
    """Tables found only in the right dataset, which aren't compared."""
    summary: PudlDiffSummary
    """Totals over all the compared tables."""
    tables: dict[str, TableDiffReport]
    """Each compared table's report, keyed by its name in the left dataset."""

    error: str | None = None
    """Why the comparison as a whole failed, e.g. no tables could be listed. This
    is ``None`` when the only failures are of individual tables, which each
    record their own :attr:`~.TableDiffReport.error`."""

    @pydantic.computed_field
    @property
    def success(self) -> bool:
        """Whether the comparison completed.

        That is, :attr:`error` is ``None`` and so is every table's. Distinct from
        :attr:`is_identical`: a comparison can succeed and still find differences.
        """
        return self.error is None and all(t.success for t in self.tables.values())

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether every compared table is identical, and :attr:`success` is ``True``.

        Tables found in only one dataset don't count against this.
        """
        return self.success and all(t.is_identical for t in self.tables.values())

    @property
    def exit_code(self) -> int:
        """The CLI's exit status for this report.

        ``0`` if everything is identical, ``1`` if any differ, ``2`` if any
        comparison failed.
        """
        if not self.success:
            return 2
        return 0 if self.is_identical else 1


def build_pudl_diff_report(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    tables: dict[str, TableDiffReport],
    *,
    options: DiffOptions | None = None,
    tables_only_in_left: Iterable[str] = (),
    tables_only_in_right: Iterable[str] = (),
    elapsed_seconds: float | None = None,
    error: str | None = None,
) -> PudlDiffReport:
    """Assemble the report on a comparison of two whole datasets.

    Args:
        left: The "left" dataset that was compared.
        right: The "right" dataset compared against it.
        tables: The report on each compared table, keyed by its name in ``left``.
        options: The settings the comparison ran with.
        tables_only_in_left: Tables that weren't compared as they aren't in
            ``right``.
        tables_only_in_right: Likewise, for tables not in ``left``.
        elapsed_seconds: How long the whole comparison took.
        error: Why the comparison failed as a whole, if it did - e.g. because
            no tables could be found to compare.
    """

    def _info(dataset: PudlDiffDataset) -> DatasetInfo:
        try:
            provenance = dataset.provenance()
        except OSError, json.JSONDecodeError:
            provenance = DatasetProvenance()
        return DatasetInfo(root=dataset.display_root, **provenance.model_dump())

    return PudlDiffReport(
        created=datetime.now(UTC).isoformat(),
        elapsed_seconds=elapsed_seconds,
        left_dataset=_info(left),
        right_dataset=_info(right),
        options=options or DiffOptions(),
        tables_only_in_left=sorted(tables_only_in_left),
        tables_only_in_right=sorted(tables_only_in_right),
        summary=PudlDiffSummary.from_tables(tables),
        tables=tables,
        error=error,
    )


@dataclass(frozen=True)
class TableOutcome:
    """What happened when comparing one table, for display."""

    table_name: str
    exit_code: int
    """``0`` if identical, ``1`` if different, ``2`` if the comparison failed."""
    elapsed_seconds: float | None
    error: str | None
    rows: RowChanges
    sizes: SizeComparison
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


def table_outcome(table_name: str, report: TableDiffReport) -> TableOutcome:
    """Boil a table's report down to what we display."""
    exit_code = 0 if report.is_identical else 1
    if not report.success:
        exit_code = 2
    row_counts = report.row_count_diff
    schema = report.schema_diff
    return TableOutcome(
        table_name=table_name,
        exit_code=exit_code,
        elapsed_seconds=report.elapsed_seconds,
        error=report.error,
        rows=RowChanges.from_summary(report.row_diff),
        sizes=report,
        left_rows=row_counts.left_row_count if row_counts else None,
        right_rows=row_counts.right_row_count if row_counts else None,
        left_columns=schema.left_column_count if schema else None,
        columns_added=len(schema.columns_only_in_right) if schema else None,
        columns_removed=len(schema.columns_only_in_left) if schema else None,
        dtypes_changed=len(schema.dtype_changes) if schema else 0,
        peak_rss_bytes=report.peak_rss_bytes,
    )
