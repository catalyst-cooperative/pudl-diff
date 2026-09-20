"""The report on the comparison of one table.

Serializable summaries of the schema, row-count and row-level differences between two
tables, assembled into a :class:`TableDiffReport`, and :func:`report_table_diff`, which
compares a table and builds its report.
"""

import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Literal

import pydantic

import pudl.logging_helpers
from pudl.validate.diff.base import ReportModel
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.formatting import format_bytes
from pudl.validate.diff.outputs import (
    ParquetOutput,
    RowDiffParquetOutputs,
    write_row_diff_parquet,
)
from pudl.validate.diff.row_counts import (
    RowCountDiff,
)
from pudl.validate.diff.rows import (
    KeyedRowDiff,
    RowSetDiff,
)
from pudl.validate.diff.schema import SchemaDiff
from pudl.validate.diff.table import (
    MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
    RowComparisonSkipReason,
    TableDiffRun,
    run_table_diff,
)

logger = pudl.logging_helpers.get_logger(__name__)


class SchemaDiffSummary(ReportModel):
    """The JSON-report form of :class:`~.SchemaDiff`."""

    columns_only_in_left: list[str]
    """Names of the columns that are only in the left table: removed columns."""
    columns_only_in_right: list[str]
    """Names of the columns that are only in the right table: added columns."""
    dtype_changes: dict[str, tuple[str, str]]
    """Maps column name to a ``(left_dtype, right_dtype)`` pair of dtype
    names, e.g. ``("Int64", "Int32")``."""
    left_column_count: int
    """Number of columns in the left table."""
    right_column_count: int
    """Number of columns in the right table."""

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether the two schemas have the same columns and dtypes."""
        return not (
            self.columns_only_in_left
            or self.columns_only_in_right
            or self.dtype_changes
        )

    @classmethod
    def from_schema_diff(cls, schema_diff: SchemaDiff) -> SchemaDiffSummary:
        """Build from a :class:`~.SchemaDiff`, stringifying its Polars dtypes."""
        return cls(
            columns_only_in_left=schema_diff.columns_only_in_left,
            columns_only_in_right=schema_diff.columns_only_in_right,
            dtype_changes={
                name: (str(left_dtype), str(right_dtype))
                for name, (
                    left_dtype,
                    right_dtype,
                ) in schema_diff.dtype_changes.items()
            },
            left_column_count=schema_diff.left_column_count,
            right_column_count=schema_diff.right_column_count,
        )


def _json_partition(partition: Any) -> int | float | bool | str | None:
    """A partition value as a native JSON type if it has one, else its ``str()``."""
    if partition is None or isinstance(partition, int | float | bool | str):
        return partition
    return str(partition)


class PartitionRowCountChange(ReportModel):
    """A single partition's row-count change, for the JSON report."""

    partition: int | float | bool | str | None
    """The partition value, as a native JSON type where there is one (e.g. the
    integer ``2026`` for a year partition), and as its ``str()`` otherwise (e.g.
    an ISO-formatted date). ``None`` only for a genuinely null partition value
    (e.g. a table with nulls in its partition column), never to mean "no
    partitioning" - see :attr:`RowCountDiffSummary.changes`."""
    left_row_count: int | None
    """``None`` if this partition is missing entirely from the left table."""
    right_row_count: int | None
    """``None`` if this partition is missing entirely from the right table."""
    row_count_difference: int
    """``right_row_count - left_row_count`` for this partition, counting a
    partition missing from one side as having no rows there."""


class RowCountDiffSummary(ReportModel):
    """The JSON-report form of :class:`~.RowCountDiff`."""

    left_row_count: int
    """Total number of rows in the left table."""
    right_row_count: int
    """Total number of rows in the right table."""
    row_count_difference: int
    """``right_row_count - left_row_count``: the change in row count from the
    reference (left) table to the right table."""
    partition_expr: str | None
    """The partition column/expression used, or ``None`` if row counts were
    compared as a single whole-table total. For a partition looked up from dbt,
    this is the SQL expression from the dbt schema file, e.g.
    ``EXTRACT(YEAR FROM report_date)``."""
    changes: list[PartitionRowCountChange]
    """Empty whenever :attr:`partition_expr` is ``None``: with no
    partitioning there's only ever one (whole-table) count to compare, and
    that's already captured by :attr:`left_row_count`/:attr:`right_row_count`
    above."""

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether the row counts match, overall and in every partition."""
        return self.left_row_count == self.right_row_count and not self.changes

    @classmethod
    def from_row_count_diff(cls, row_count_diff: RowCountDiff) -> RowCountDiffSummary:
        """Build from a :class:`~.RowCountDiff`."""
        changes = (
            [
                PartitionRowCountChange(
                    partition=_json_partition(partition),
                    left_row_count=left_row_count,
                    right_row_count=right_row_count,
                    row_count_difference=(right_row_count or 0) - (left_row_count or 0),
                )
                for partition, (
                    left_row_count,
                    right_row_count,
                ) in row_count_diff.changes.items()
            ]
            if row_count_diff.partition_expr is not None
            else []
        )
        return cls(
            left_row_count=row_count_diff.left_row_count,
            right_row_count=row_count_diff.right_row_count,
            row_count_difference=(
                row_count_diff.right_row_count - row_count_diff.left_row_count
            ),
            partition_expr=(
                row_count_diff.partition_label or str(row_count_diff.partition_expr)
                if row_count_diff.partition_expr is not None
                else None
            ),
            changes=changes,
        )


class ParquetOutputSummary(ReportModel):
    """The JSON-report form of a single :class:`~.ParquetOutput`."""

    path: str
    """Where the file was written, under the comparison's output directory."""
    bytes: int
    """Size of the file in bytes."""
    hash: str
    """``"sha256:<hexdigest>"`` of the file's contents, the convention PUDL's
    ``datapackage.json`` uses for its own resource files."""

    @pydantic.computed_field
    @property
    def size(self) -> str:
        """:attr:`bytes` in human-readable form, e.g. ``12.3 MB``."""
        return format_bytes(self.bytes)

    @classmethod
    def from_parquet_output(cls, output: ParquetOutput) -> ParquetOutputSummary:
        """Build from a :class:`~.ParquetOutput`."""
        return cls(path=str(output.path), bytes=output.bytes, hash=output.hash)


#: Reasons one *section* of the JSON report's ``row_diff`` (``pk_diff`` or
#: ``non_pk_diff``) has no summary. The first three are the
#: :data:`~.RowComparisonSkipReason` values, for when the whole row-level comparison
#: was skipped; the last two mean that section simply doesn't apply, because the
#: table does or doesn't have a primary key.
RowDiffSectionSkipReason = Literal[
    "too_many_rows",
    "incompatible_dtypes",
    "mismatched_columns",
    "primary_key_available",
    "no_primary_key",
]


class RowDiffSectionSkipped(ReportModel):
    """Stands in for a row diff summary that wasn't produced, and says why."""

    status: Literal["skipped"] = "skipped"
    """Always ``"skipped"``: this is what tells this apart from a full summary."""
    skipped_reason: RowDiffSectionSkipReason
    """Why there is no summary in this section:

    * ``too_many_rows``: either table has more rows than
      :attr:`DiffOptions.max_compare_rows`, so no row-level comparison was made.
    * ``incompatible_dtypes``: the row-level comparison failed, most likely because
      the tables' columns have incompatible dtypes.
    * ``mismatched_columns``: the tables have different columns and no primary key, so
      their rows can't be compared meaningfully.
    * ``primary_key_available``: not skipped for a problem. The table has a primary
      key, so its comparison is in ``pk_diff``, not ``non_pk_diff``.
    * ``no_primary_key``: likewise, the table has no primary key, so its comparison is
      in ``non_pk_diff``, not ``pk_diff``.
    """


class PkRowDiffSummary(ReportModel):
    """The JSON-report form of a :class:`~.KeyedRowDiff`."""

    status: Literal["compared"] = "compared"
    """Always ``"compared"``: this is what tells this apart from a skipped section."""
    primary_key_columns: list[str]
    """The names of the table's primary key columns."""
    only_in_left_count: int
    """Number of rows whose primary key is only in the left table: removed rows."""
    only_in_right_count: int
    """Number of rows whose primary key is only in the right table: added rows."""
    changed_row_count: int
    """Number of shared-primary-key rows with at least one differing
    non-primary-key value."""
    column_changes: dict[str, int]
    """Maps each non-primary-key column to the number of shared-primary-key rows
    where its value differs between the tables. Columns with no changes are
    omitted."""

    @pydantic.computed_field
    @property
    def primary_keys_identical(self) -> bool:
        """Whether both tables have the same set of primary keys."""
        return self.only_in_left_count == 0 and self.only_in_right_count == 0

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether the primary keys match and no shared-key row has changed."""
        return self.primary_keys_identical and not self.column_changes


class NonPkRowDiffSummary(ReportModel):
    """The JSON-report form of a :class:`~.RowSetDiff` without a primary key."""

    status: Literal["compared"] = "compared"
    """Always ``"compared"``: this is what tells this apart from a skipped section."""
    only_in_left_count: int
    """Number of rows only in the left table: removed rows. Rows are counted as a
    multiset, so if a row appears more times on the left, the surplus copies count."""
    only_in_right_count: int
    """Number of rows only in the right table: added rows, counted the same way."""
    symmetric_difference_count: int
    """``only_in_left_count + only_in_right_count``. Counts rows as a
    multiset, so surplus copies of duplicated rows are included."""
    multiplicity_changed_row_count: int
    """Number of distinct rows present in both tables, but a different number
    of times."""

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether every row in one table has a matching row in the other."""
        return self.only_in_left_count == 0 and self.only_in_right_count == 0


class RowDiffSummary(ReportModel):
    """The JSON-report form of :attr:`~.TableDiffResult.row_diff`.

    Exactly one of :attr:`pk_diff` and :attr:`non_pk_diff` is a full summary
    (unless row-level comparison was skipped entirely); the other is a
    :class:`RowDiffSectionSkipped` saying why it wasn't produced: ``no_primary_key``
    or ``primary_key_available`` when the table's primary key determined which
    kind of comparison applies, or the reason the comparison was skipped
    altogether (see :data:`~.RowComparisonSkipReason`), in which case the one that would
    have run carries that reason.
    """

    pk_diff: Annotated[
        PkRowDiffSummary | RowDiffSectionSkipped, pydantic.Field(discriminator="status")
    ]
    """The row-level comparison of a table with a primary key: a full summary
    (``status`` is ``"compared"``), or the reason there isn't one (``"skipped"``)."""
    non_pk_diff: Annotated[
        NonPkRowDiffSummary | RowDiffSectionSkipped,
        pydantic.Field(discriminator="status"),
    ]
    """The row-level comparison of a table without a primary key: a full summary
    (``status`` is ``"compared"``), or the reason there isn't one (``"skipped"``)."""
    left_only_parquet: ParquetOutputSummary | None = None
    """The Parquet file of the rows found only in the left table (for a table with a
    primary key, this includes the left-hand values of rows that changed), or ``None``
    if no file was written."""
    right_only_parquet: ParquetOutputSummary | None = None
    """The same, for the right table."""


def _build_row_diff_summary(
    row_diff: RowSetDiff | KeyedRowDiff | None,
    pk_cols: Sequence[str],
    skipped_reason: RowComparisonSkipReason | None,
    parquet_outputs: RowDiffParquetOutputs | None,
) -> RowDiffSummary:
    pk_diff_summary: PkRowDiffSummary | RowDiffSectionSkipped
    non_pk_diff_summary: NonPkRowDiffSummary | RowDiffSectionSkipped
    if isinstance(row_diff, KeyedRowDiff):
        pk_diff_summary = PkRowDiffSummary(
            primary_key_columns=list(pk_cols),
            only_in_left_count=row_diff.pk_diff.only_in_left_count,
            only_in_right_count=row_diff.pk_diff.only_in_right_count,
            changed_row_count=row_diff.changed_row_count,
            column_changes=row_diff.column_changes,
        )
        non_pk_diff_summary = RowDiffSectionSkipped(
            skipped_reason="primary_key_available"
        )
    elif isinstance(row_diff, RowSetDiff):
        pk_diff_summary = RowDiffSectionSkipped(skipped_reason="no_primary_key")
        non_pk_diff_summary = NonPkRowDiffSummary(
            only_in_left_count=row_diff.only_in_left_count,
            only_in_right_count=row_diff.only_in_right_count,
            symmetric_difference_count=(
                row_diff.only_in_left_count + row_diff.only_in_right_count
            ),
            multiplicity_changed_row_count=row_diff.multiplicity_changed_row_count,
        )
    else:
        if skipped_reason is None:
            raise AssertionError(
                "Row-level comparison produced no result but gave no skip reason."
            )
        if pk_cols:
            pk_diff_summary = RowDiffSectionSkipped(skipped_reason=skipped_reason)
            non_pk_diff_summary = RowDiffSectionSkipped(
                skipped_reason="primary_key_available"
            )
        else:
            pk_diff_summary = RowDiffSectionSkipped(skipped_reason="no_primary_key")
            non_pk_diff_summary = RowDiffSectionSkipped(skipped_reason=skipped_reason)
    return RowDiffSummary(
        pk_diff=pk_diff_summary,
        non_pk_diff=non_pk_diff_summary,
        left_only_parquet=(
            ParquetOutputSummary.from_parquet_output(parquet_outputs.left)
            if parquet_outputs is not None
            else None
        ),
        right_only_parquet=(
            ParquetOutputSummary.from_parquet_output(parquet_outputs.right)
            if parquet_outputs is not None
            else None
        ),
    )


@dataclass(frozen=True)
class RowChanges:
    """The row-level results of a table comparison, boiled down to counts."""

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
    """``None`` if that isn't known, e.g. because the comparison failed."""

    @classmethod
    def from_summary(cls, row_diff: RowDiffSummary | None) -> RowChanges:
        """Boil a table's row diff summary down to its counts."""
        if row_diff is None:
            return cls()
        pk_diff, non_pk_diff = row_diff.pk_diff, row_diff.non_pk_diff
        if isinstance(pk_diff, PkRowDiffSummary):
            return cls(
                added=pk_diff.only_in_right_count,
                changed=pk_diff.changed_row_count,
                removed=pk_diff.only_in_left_count,
                has_primary_key=True,
            )
        if isinstance(non_pk_diff, NonPkRowDiffSummary):
            return cls(
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
        return cls(
            skipped_reason=reasons[0] if reasons else None,
            has_primary_key=non_pk_diff.skipped_reason == "primary_key_available",
        )


class SizeComparison(ReportModel):
    """The sizes of the left and right side of a comparison, and how they differ.

    Sizes are bytes on disk (or in cloud storage) of the Parquet file(s) being
    compared. Everything but the two ``*_table_bytes`` fields is derived from them
    when the report is serialized, and is ``None`` whenever either size is unknown.
    """

    left_table_bytes: int | None = None
    """Size in bytes of the left table's Parquet file(s), or ``None`` if unknown."""
    right_table_bytes: int | None = None
    """Size in bytes of the right table's Parquet file(s), or ``None`` if unknown."""

    @pydantic.computed_field
    @property
    def left_table_size(self) -> str | None:
        """:attr:`left_table_bytes` in human-readable form, e.g. ``12.3 MB``."""
        if self.left_table_bytes is None:
            return None
        return format_bytes(self.left_table_bytes)

    @pydantic.computed_field
    @property
    def right_table_size(self) -> str | None:
        """:attr:`right_table_bytes` in human-readable form."""
        if self.right_table_bytes is None:
            return None
        return format_bytes(self.right_table_bytes)

    @pydantic.computed_field
    @property
    def bytes_difference(self) -> int | None:
        """The change in size from the left to the right table.

        ``right_table_bytes - left_table_bytes``, so negative if the right side is
        smaller. Compression changes show up here even if the contents don't.
        """
        if self.left_table_bytes is None or self.right_table_bytes is None:
            return None
        return self.right_table_bytes - self.left_table_bytes

    @pydantic.computed_field
    @property
    def bytes_difference_size(self) -> str | None:
        """:attr:`bytes_difference` in human-readable form, e.g. ``-1.2 MB``."""
        if self.bytes_difference is None:
            return None
        return format_bytes(self.bytes_difference, signed=True)

    @pydantic.computed_field
    @property
    def bytes_difference_percent(self) -> float | None:
        """:attr:`bytes_difference` as a percentage of :attr:`left_table_bytes`.

        ``None`` if the left size is unknown or zero.
        """
        if self.bytes_difference is None or not self.left_table_bytes:
            return None
        return round(100 * self.bytes_difference / self.left_table_bytes, 4)


class TableDiffReport(SizeComparison):
    """A single table comparison, in the form saved in the PUDL Diff JSON report.

    Built by :func:`build_table_diff_report` from a :class:`~.TableDiffRun`, and
    one entry in :attr:`~.PudlDiffReport.tables`. Fields that describe the whole
    comparison of the two datasets (when it was run, the datasets' provenance) live
    on the :class:`~.PudlDiffReport` instead. Contains no row-level data itself -
    only counts and summaries; the actual differing rows are written
    separately as Parquet files (see :func:`~.write_row_diff_parquet`) and
    referenced from :attr:`row_diff`.
    """

    left_table_name: str
    """The name of the table in the left dataset."""
    left_table_path: str
    """The path or URL of the table's Parquet file in the left dataset. Worked out
    from the dataset's root and the table's name, so it is given even if the file
    doesn't exist, e.g. because the comparison failed."""
    right_table_name: str
    """The name of the table in the right dataset. Differs from
    :attr:`left_table_name` only when two differently named tables were compared,
    e.g. a ``core_`` table against the ``out_`` table built from it."""
    right_table_path: str
    """The path or URL of the table's Parquet file in the right dataset."""
    elapsed_seconds: float | None = None
    """Wall-clock time the comparison of this table took, or ``None`` if it failed."""
    peak_rss_bytes: int | None = None
    """The most memory (resident set size) the process used during this comparison
    beyond what it was using when the comparison started, in bytes. Sampled, so a
    very short spike could be missed. ``None`` if the comparison failed."""
    peak_cpu_percent: float | None = None
    """The highest CPU utilization sampled during this comparison, as a percentage of
    one core: ``400.0`` means four cores kept fully busy. A rough gauge of how
    parallel the work was. ``None`` if the comparison failed."""

    schema_diff: SchemaDiffSummary | None = None
    """How the tables' columns and dtypes differ, or ``None`` if the comparison
    failed."""
    row_count_diff: RowCountDiffSummary | None = None
    """How the tables' row counts differ, or ``None`` if the comparison failed."""
    row_diff: RowDiffSummary | None = None
    """How the tables' rows differ, or ``None`` if the comparison failed. If the
    row-level comparison was skipped, its sections say why."""

    error: str | None = None
    """Exception message plus traceback, if the comparison failed to
    complete. ``None`` if :attr:`success` is ``True``."""

    @pydantic.computed_field
    @property
    def success(self) -> bool:
        """Whether the comparison completed at all, successfully or not.

        See :class:`~.TableDiffRun`. Distinct from :attr:`is_identical`: a
        comparison can succeed and still find the tables different.
        """
        return self.error is None

    @pydantic.computed_field
    @property
    def is_identical(self) -> bool:
        """Whether the table is functionally identical between the two datasets.

        Conservatively ``False`` whenever :attr:`success` is ``False``, since a
        failed comparison can't establish that the tables are identical, and
        whenever the row-level comparison didn't run (it was skipped), since then
        the rows are unverified even if the schema and row counts match.
        """
        row_diff = self.row_diff
        return (
            self.success
            and self.schema_diff is not None
            and self.schema_diff.is_identical
            and self.row_count_diff is not None
            and self.row_count_diff.is_identical
            and row_diff is not None
            and (
                (
                    isinstance(row_diff.pk_diff, PkRowDiffSummary)
                    and row_diff.pk_diff.is_identical
                )
                or (
                    isinstance(row_diff.non_pk_diff, NonPkRowDiffSummary)
                    and row_diff.non_pk_diff.is_identical
                )
            )
        )

    @pydantic.computed_field
    @property
    def peak_rss(self) -> str | None:
        """:attr:`peak_rss_bytes` in human-readable form, e.g. ``1.2 GB``."""
        if self.peak_rss_bytes is None:
            return None
        return format_bytes(self.peak_rss_bytes)


def _table_bytes(dataset: PudlDiffDataset, table_name: str) -> int | None:
    """The size of a table's file, or ``None`` if it can't be determined.

    Sizes are reported on a best-effort basis, even for comparisons that failed
    - often *because* the file doesn't exist or can't be reached.
    """
    try:
        return dataset.table_bytes(table_name)
    except Exception:  # noqa: BLE001
        logger.debug(f"Couldn't get the size of {table_name!r}.", exc_info=True)
        return None


def build_table_diff_report(
    run: TableDiffRun,
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_name: str,
    *,
    right_table_name: str | None = None,
    parquet_outputs: RowDiffParquetOutputs | None = None,
) -> TableDiffReport:
    """Build the JSON-report form of a table comparison.

    Args:
        run: The comparison's outcome, from :func:`~.run_table_diff`.
        left: The "left" dataset that was compared.
        right: The "right" dataset compared against it.
        table_name: Name of the table compared in ``left``.
        right_table_name: Name of the table compared in ``right``, if it
            differed from ``table_name``. Defaults to ``table_name``.
        parquet_outputs: The Parquet side-output files written for this
            table's row diff, from :func:`~.write_row_diff_parquet`, if any
            were written.
    """
    right_table_name = right_table_name or table_name

    def _table_path(dataset: PudlDiffDataset, name: str) -> str:
        # table_path() is deterministic from root + name alone and never
        # raises, so it's always reportable even when the table turns out
        # not to exist there (e.g. an unknown table name, or a comparison
        # that failed before that could be confirmed).
        return str(dataset.table_path(name))

    if not run.success or run.result is None:
        return TableDiffReport(
            left_table_name=table_name,
            left_table_path=_table_path(left, table_name),
            left_table_bytes=_table_bytes(left, table_name),
            right_table_name=right_table_name,
            right_table_path=_table_path(right, right_table_name),
            right_table_bytes=_table_bytes(right, right_table_name),
            error=run.error,
        )

    result = run.result
    pk_cols = left.primary_key(table_name)
    row_diff_summary = _build_row_diff_summary(
        result.row_diff, pk_cols, result.row_diff_skipped_reason, parquet_outputs
    )
    return TableDiffReport(
        left_table_name=result.table_name,
        left_table_path=_table_path(left, result.table_name),
        left_table_bytes=_table_bytes(left, result.table_name),
        right_table_name=result.right_table_name,
        right_table_path=_table_path(right, result.right_table_name),
        right_table_bytes=_table_bytes(right, result.right_table_name),
        elapsed_seconds=result.elapsed_seconds,
        peak_rss_bytes=result.peak_rss_bytes,
        peak_cpu_percent=result.peak_cpu_percent,
        schema_diff=SchemaDiffSummary.from_schema_diff(result.schema_diff),
        row_count_diff=RowCountDiffSummary.from_row_count_diff(result.row_count_diff),
        row_diff=row_diff_summary,
        error=None,
    )


class DiffOptions(ReportModel):
    """The settings a dataset comparison was run with.

    Recorded in the report because they affect how its results should be
    interpreted, e.g. whether a table's row-level comparison was skipped.
    """

    rtol: float = 1e-5
    """Relative tolerance for float equality, as in :func:`numpy.isclose`."""
    atol: float = 1e-8
    """Absolute tolerance for float equality, as in :func:`numpy.isclose`."""
    max_compare_rows: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON
    """Row-level comparison is skipped for any table with more rows than this."""
    max_output_rows: int | None = None
    """Cap on the rows written to each Parquet side-output file, or ``None`` to
    write every differing row."""
    auto_partition: bool = True
    """Whether row counts are grouped by the partition from PUDL's dbt row-count
    tests, where a table has one."""
    partition_expr: str | None = None
    """Column to group row counts by, for every table, overriding the dbt
    configured partitions."""


def report_table_diff(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_name: str,
    output_path: str | os.PathLike[str],
    *,
    right_table_name: str | None = None,
    options: DiffOptions | None = None,
) -> TableDiffReport:
    """Compare a table, write its Parquet side-outputs, and report on it.

    Never raises because the comparison failed: see :func:`~.run_table_diff`.

    Args:
        left: The "left" dataset to compare.
        right: The "right" dataset to compare against ``left``.
        table_name: Name of the table to compare in ``left``.
        output_path: Directory to write the differing rows' Parquet files into.
        right_table_name: Name of the table to compare in ``right``, if it differs
            from ``table_name``.
        options: How to run the comparison. Defaults to :class:`DiffOptions`'s.
    """
    options = options or DiffOptions()
    run = run_table_diff(
        left,
        right,
        table_name,
        right_table_name=right_table_name,
        partition_expr=options.partition_expr,
        auto_partition=options.auto_partition,
        rtol=options.rtol,
        atol=options.atol,
        max_rows_for_row_level_comparison=options.max_compare_rows,
    )
    parquet_outputs = None
    if run.success and run.result is not None:
        parquet_outputs = write_row_diff_parquet(
            run.result.row_diff,
            output_path,
            table_name,
            right_table_name=run.result.right_table_name,
            max_rows_per_output_parquet=options.max_output_rows,
        )
    return build_table_diff_report(
        run,
        left,
        right,
        table_name,
        right_table_name=right_table_name,
        parquet_outputs=parquet_outputs,
    )
