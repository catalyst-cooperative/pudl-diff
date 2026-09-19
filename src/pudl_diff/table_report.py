"""Compare PUDL Parquet outputs between two dataset roots.

A "root" is a local or remote directory containing the Parquet outputs of a full
PUDL ETL run, along with a datapackage descriptor of those outputs (e.g.
``$PUDL_OUTPUT/parquet`` or ``s3://pudl.catalyst.coop/nightly``). This module lets
callers load and compare individual tables between two such roots.
"""

import dataclasses
import hashlib
import json
import os
import time
import traceback
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
import pydantic

import pudl.logging_helpers
from pudl.validate.diff.dataset import DatasetProvenance, PudlDiffDataset
from pudl.validate.diff.formatting import format_bytes
from pudl.validate.diff.performance import _PerformanceSampler
from pudl.validate.diff.row_counts import (
    RowCountDiff,
    compare_row_counts,
    dbt_partition_expr_to_polars,
    get_dbt_partition_expr,
)
from pudl.validate.diff.rows import (
    KeyedRowDiff,
    RowSetDiff,
    compare_rows_with_pk,
    compare_rows_without_pk,
)
from pudl.validate.diff.schema import SchemaDiff, compare_schemas

logger = pudl.logging_helpers.get_logger(__name__)


#: Fixed set of reasons :func:`compare_table` skips the row-level comparison
#: *altogether* (see :attr:`TableDiffResult.row_diff_skipped_reason`). A detailed,
#: interpolated explanation (row counts, table names, etc.) is still logged
#: via ``logger.warning`` at the point of the skip; this only carries the
#: category, so report consumers can branch on it without parsing text.
RowComparisonSkipReason = Literal[
    "too_many_rows", "incompatible_dtypes", "mismatched_columns"
]


#: Row-level comparisons use Polars' streaming engine, spill their results to
#: disk, and join narrow 64-bit row hashes rather than whole rows, but the joins
#: still hold a few bytes per row in memory (and every row, if all of a table's
#: float values differ slightly), so above this many rows on either side they
#: risk exhausting memory on typical hardware. :func:`compare_table` skips
#: row-level comparison entirely once either table exceeds this, rather than risk
#: an out-of-memory crash; the cheaper schema and row-count comparisons still run.
MAX_ROWS_FOR_ROW_LEVEL_COMPARISON = 100_000_000


@dataclass(frozen=True)
class TableDiffResult:
    """The full comparison of a single table between two PUDL datasets."""

    table_name: str
    right_table_name: str
    """Same as :attr:`table_name` unless a different ``right_table_name`` was
    passed to :func:`compare_table`, e.g. to compare a `core_` table against
    the `out_` table built from it."""
    schema_diff: SchemaDiff
    row_count_diff: RowCountDiff
    row_diff: RowSetDiff | KeyedRowDiff | None
    """For a table with a primary key, computed over only the columns shared
    by both datasets if their column sets differ (with a warning logged), since
    the primary key still uniquely identifies rows either way. ``None`` if the
    table has no primary key and the column sets differ - a row-level
    comparison across changed columns isn't meaningful without one - if
    either table has more than the configured row-level comparison cap, or if
    the comparison didn't complete for some other reason; see
    :func:`compare_table` and :attr:`row_diff_skipped_reason`."""
    row_diff_skipped_reason: RowComparisonSkipReason | None = None
    """Why :attr:`row_diff` is ``None``: too many rows, incompatible column
    dtypes, or changed columns without a usable primary key. ``None`` if
    row-level comparison actually ran (whether or not it found any diffs)."""
    elapsed_seconds: float = 0.0
    """Wall-clock time :func:`compare_table` took to run this comparison."""
    peak_rss_bytes: int = 0
    """Peak resident set size attributable to this comparison: the highest
    whole-process RSS observed while :func:`compare_table` was running, net
    of the process's RSS just before it started. Sampled on a background
    thread (see :class:`_PerformanceSampler`), so very short, sharp spikes
    between samples may be missed."""
    peak_cpu_percent: float = 0.0
    """Peak per-interval CPU utilization observed while :func:`compare_table`
    was running, as a percent of one core (e.g. ``400.0`` for four cores kept
    fully busy at once). Sampled on the same background thread as
    :attr:`peak_rss_bytes`; a rough gauge of how parallelized Polars' work
    was, not an exact thread count."""

    @property
    def is_identical(self) -> bool:
        """Whether the table is functionally identical between the two datasets.

        Conservatively ``False`` whenever :attr:`row_diff` is ``None``, even if
        the schema and row counts match: without a row-level comparison having
        actually run, row content is unverified and can't be called identical.
        """
        return (
            self.schema_diff.is_identical
            and self.row_count_diff.is_identical
            and self.row_diff is not None
            and self.row_diff.is_identical
        )


def compare_table(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_name: str,
    *,
    right_table_name: str | None = None,
    partition_expr: str | pl.Expr | None = None,
    auto_partition: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
) -> TableDiffResult:
    """Compare a single table between two PUDL datasets.

    Times the comparison and samples this process's peak RSS and CPU
    utilization while it runs (see :class:`_PerformanceSampler`), recording
    all three on the returned :class:`TableDiffResult`. See
    :func:`_compare_table` for the comparison logic itself.
    """
    start = time.perf_counter()
    with _PerformanceSampler() as sampler:
        result = _compare_table(
            left,
            right,
            table_name,
            right_table_name=right_table_name,
            partition_expr=partition_expr,
            auto_partition=auto_partition,
            rtol=rtol,
            atol=atol,
            max_rows_for_row_level_comparison=max_rows_for_row_level_comparison,
        )
    return dataclasses.replace(
        result,
        elapsed_seconds=time.perf_counter() - start,
        peak_rss_bytes=sampler.peak_rss_bytes,
        peak_cpu_percent=sampler.peak_cpu_percent,
    )


def _compare_table(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_name: str,
    *,
    right_table_name: str | None = None,
    partition_expr: str | pl.Expr | None = None,
    auto_partition: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
) -> TableDiffResult:
    """Compare a single table between two PUDL datasets.

    Runs the schema and row-count comparisons, then dispatches to
    :func:`compare_rows_with_pk` or :func:`compare_rows_without_pk` depending on
    whether ``table_name`` has a primary key (per ``left``'s datapackage).

    Args:
        left: The "left" dataset to compare.
        right: The "right" dataset to compare against ``left``.
        table_name: Name of the table to compare in ``left``, and in ``right``
            too unless ``right_table_name`` is given.
        right_table_name: Name of the table to compare in ``right``, if it
            differs from ``table_name`` - e.g. comparing a `core_` table
            against the `out_` table built from it. Defaults to ``table_name``.
            The primary key and dbt partition configuration are still looked
            up under ``table_name``, so this assumes ``right_table_name``'s
            schema is compatible enough to share them (e.g. sharing the same
            primary key columns).
        partition_expr: Column name or Polars expression to group row counts by.
            If not given and ``auto_partition`` is ``True`` (the default), it's
            derived from PUDL's dbt row-count test configuration via
            :func:`get_partition_expr_for_table`; if that table has no such
            test, row counts are compared as a single whole-table total.
        auto_partition: Whether to look up ``partition_expr`` automatically as
            described above when it isn't given explicitly. Set to ``False``
            to compare whole-table row counts even for a table that has a dbt
            partition configured.
        rtol: Relative tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default.
        atol: Absolute tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default.
        max_rows_for_row_level_comparison: Row-level comparison is skipped
            entirely, logging a warning, whenever either table exceeds this
            many rows (see :data:`MAX_ROWS_FOR_ROW_LEVEL_COMPARISON`).
    """
    right_table_name = right_table_name or table_name
    label = (
        repr(table_name)
        if right_table_name == table_name
        else f"{table_name!r} (left) vs {right_table_name!r} (right)"
    )

    left_lf = left.scan_table(table_name)
    right_lf = right.scan_table(right_table_name)
    left_schema = left_lf.collect_schema()
    right_schema = right_lf.collect_schema()

    schema_diff = compare_schemas(left_schema, right_schema)

    resolved_partition_expr = partition_expr
    partition_label = None
    if resolved_partition_expr is None and auto_partition:
        # Report the dbt SQL expression itself, so it can be traced back to the
        # dbt schema file it came from.
        partition_label = get_dbt_partition_expr(table_name)
        if partition_label is not None:
            partition_label = partition_label.strip()
            resolved_partition_expr = dbt_partition_expr_to_polars(partition_label)
    row_count_diff = compare_row_counts(
        left_lf,
        right_lf,
        partition_expr=resolved_partition_expr,
        partition_label=partition_label,
    )

    row_diff: RowSetDiff | KeyedRowDiff | None = None
    skip_reason: RowComparisonSkipReason | None = None
    left_row_count = row_count_diff.left_row_count
    right_row_count = row_count_diff.right_row_count
    if max(left_row_count, right_row_count) > max_rows_for_row_level_comparison:
        logger.warning(
            f"{label} has more than {max_rows_for_row_level_comparison:,} rows "
            f"({left_row_count:,} left, {right_row_count:,} right); skipping "
            "row-level comparison to avoid exhausting memory."
        )
        return TableDiffResult(
            table_name=table_name,
            right_table_name=right_table_name,
            schema_diff=schema_diff,
            row_count_diff=row_count_diff,
            row_diff=None,
            row_diff_skipped_reason="too_many_rows",
        )

    same_columns = (
        not schema_diff.columns_only_in_left and not schema_diff.columns_only_in_right
    )
    pk_cols = left.primary_key(table_name)
    if not same_columns and pk_cols:
        shared_columns = [name for name in left_schema if name in right_schema]
        if set(pk_cols) <= set(shared_columns):
            logger.warning(
                f"{label} has different columns between the two datasets; "
                "comparing rows using only the columns shared by both."
            )
            left_lf = left_lf.select(shared_columns)
            right_lf = right_lf.select(shared_columns)
            same_columns = True
        else:
            logger.warning(
                f"{label}'s primary key columns aren't all present in both "
                "datasets; skipping row-level comparison."
            )
            skip_reason = "mismatched_columns"

    if same_columns:
        try:
            if pk_cols:
                row_diff = compare_rows_with_pk(
                    left_lf,
                    right_lf,
                    pk_cols,
                    rtol=rtol,
                    atol=atol,
                )
            else:
                row_diff = compare_rows_without_pk(
                    left_lf, right_lf, rtol=rtol, atol=atol
                )
        except pl.exceptions.PolarsError:
            logger.warning(
                f"Row-level comparison for {label} failed, likely due to "
                "incompatible column dtypes between the two tables; skipping it."
            )
            skip_reason = "incompatible_dtypes"
            row_diff = None
    elif not pk_cols:
        logger.warning(
            f"{label} has different columns between the two datasets; "
            "skipping row-level comparison."
        )
        skip_reason = "mismatched_columns"

    return TableDiffResult(
        table_name=table_name,
        right_table_name=right_table_name,
        schema_diff=schema_diff,
        row_count_diff=row_count_diff,
        row_diff=row_diff,
        row_diff_skipped_reason=None if row_diff is not None else skip_reason,
    )


@dataclass(frozen=True)
class TableDiffRun:
    """The outcome of a :func:`run_table_diff` call.

    Distinct from :attr:`TableDiffResult.row_diff_skipped_reason`, which
    covers *expected* situations where row-level comparison is skipped but
    the comparison otherwise completes normally (e.g. a table too large to
    compare row-by-row). :class:`TableDiffRun` instead covers the comparison
    failing to complete at all - a missing datapackage, an S3 access
    failure, or any other unexpected error - so a report can always be
    produced even when :func:`compare_table` itself raises.
    """

    success: bool
    result: TableDiffResult | None
    """The comparison's result, or ``None`` if it failed to complete."""
    error: str | None
    """The failure's exception message plus traceback, or ``None`` if
    :attr:`success` is ``True``."""


def run_table_diff(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_name: str,
    *,
    right_table_name: str | None = None,
    partition_expr: str | pl.Expr | None = None,
    auto_partition: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
    max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
) -> TableDiffRun:
    """Run :func:`compare_table`, tolerating any failure it raises.

    Takes the same arguments as :func:`compare_table` and passes them
    through unchanged. Use this instead of calling :func:`compare_table`
    directly when building a report that should always be produced, even if
    the comparison itself blows up - e.g. because a dataset's
    ``datapackage.json`` is missing, or an S3 root is unreachable.
    """
    try:
        result = compare_table(
            left,
            right,
            table_name,
            right_table_name=right_table_name,
            partition_expr=partition_expr,
            auto_partition=auto_partition,
            rtol=rtol,
            atol=atol,
            max_rows_for_row_level_comparison=max_rows_for_row_level_comparison,
        )
    except Exception:
        logger.exception(f"Comparison of {table_name!r} failed.")
        return TableDiffRun(success=False, result=None, error=traceback.format_exc())
    return TableDiffRun(success=True, result=result, error=None)


def _row_diff_left_right_frames(
    row_diff: RowSetDiff | KeyedRowDiff,
) -> tuple[tuple[pl.LazyFrame, int], tuple[pl.LazyFrame, int]]:
    """All differing rows, split by source table, in that table's own schema.

    Returns a ``(lazy_frame, row_count)`` pair for each side.

    For a table with a primary key, this is the symmetric difference of
    primary keys (rows present on only one side) plus, for shared keys, the
    left/right values of rows whose non-PK data differs. For a table without
    one, it's just the symmetric difference of whole rows.
    """
    if isinstance(row_diff, KeyedRowDiff):
        pk_diff = row_diff.pk_diff
        left = (
            pl.concat([pk_diff.only_in_left, row_diff.changed_left]),
            pk_diff.only_in_left_count + row_diff.changed_row_count,
        )
        right = (
            pl.concat([pk_diff.only_in_right, row_diff.changed_right]),
            pk_diff.only_in_right_count + row_diff.changed_row_count,
        )
    else:
        left = (row_diff.only_in_left, row_diff.only_in_left_count)
        right = (row_diff.only_in_right, row_diff.only_in_right_count)
    return left, right


@dataclass(frozen=True)
class ParquetOutput:
    """Metadata about a single Parquet side-output file written to disk."""

    path: Path
    bytes: int
    hash: str
    """``"sha256:<hexdigest>"``, matching the convention PUDL's enriched
    ``datapackage.json`` uses for its own resource files."""
    total_row_count: int
    """Total number of differing rows on this side, before any capping by
    ``max_rows_per_output_parquet``."""
    rows_written: int
    """Number of rows actually written to :attr:`path`."""


@dataclass(frozen=True)
class RowDiffParquetOutputs:
    """The left-only and right-only Parquet files written for one table's row diff."""

    left: ParquetOutput
    right: ParquetOutput


def _sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet_output(
    lf: pl.LazyFrame,
    total_row_count: int,
    path: Path,
    max_rows_per_output_parquet: int | None,
) -> ParquetOutput:
    if max_rows_per_output_parquet is not None:
        lf = lf.head(max_rows_per_output_parquet)
    lf.sink_parquet(path, engine="streaming")
    rows_written = (
        total_row_count
        if max_rows_per_output_parquet is None
        else min(total_row_count, max_rows_per_output_parquet)
    )
    return ParquetOutput(
        path=path,
        bytes=path.stat().st_size,
        hash=f"sha256:{_sha256_of_file(path)}",
        total_row_count=total_row_count,
        rows_written=rows_written,
    )


def write_row_diff_parquet(
    row_diff: RowSetDiff | KeyedRowDiff | None,
    output_path: str | os.PathLike[str],
    table_name: str,
    *,
    right_table_name: str | None = None,
    max_rows_per_output_parquet: int | None = None,
) -> RowDiffParquetOutputs | None:
    """Write a table's differing rows to left-only/right-only Parquet files.

    Writes ``<table_name>_left_only.parquet`` and
    ``<right_table_name>_right_only.parquet`` under ``output_path`` (created
    if it doesn't exist), each matching the schema of the corresponding
    source table. For a table with a primary key, each file holds that
    side's rows from the symmetric difference of primary keys, plus that
    side's values for rows whose non-PK data differs. For a table without
    one, each file holds that side's rows from the symmetric difference of
    whole rows.

    Args:
        row_diff: The row-level comparison result to write out, e.g. from
            :attr:`TableDiffResult.row_diff`. If ``None`` - row-level
            comparison was skipped for this table - nothing is written and
            this function returns ``None``.
        output_path: Directory to write the two Parquet files into.
        table_name: Used as the filename prefix for the left output file,
            and for the right one too unless ``right_table_name`` is given.
        right_table_name: Used as the filename prefix for the right output
            file, if it differs from ``table_name`` - e.g. writing out a
            diff between a `core_` table and the `out_` table built from it,
            as passed to :func:`compare_table`. Defaults to ``table_name``.
        max_rows_per_output_parquet: If given, caps the number of rows
            written to each file. :attr:`ParquetOutput.total_row_count`
            still reflects the true (uncapped) row count.

    Returns:
        The two files' paths and metadata, or ``None`` if ``row_diff`` is
        ``None``.
    """
    if row_diff is None:
        return None
    right_table_name = right_table_name or table_name
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    (left_lf, left_count), (right_lf, right_count) = _row_diff_left_right_frames(
        row_diff
    )
    return RowDiffParquetOutputs(
        left=_write_parquet_output(
            left_lf,
            left_count,
            output_path / f"{table_name}_left_only.parquet",
            max_rows_per_output_parquet,
        ),
        right=_write_parquet_output(
            right_lf,
            right_count,
            output_path / f"{right_table_name}_right_only.parquet",
            max_rows_per_output_parquet,
        ),
    )


class SchemaDiffSummary(pydantic.BaseModel):
    """The JSON-report form of :class:`SchemaDiff`."""

    columns_only_in_left: list[str]
    columns_only_in_right: list[str]
    dtype_changes: dict[str, tuple[str, str]]
    """Maps column name to a ``(left_dtype, right_dtype)`` pair of dtype
    names, e.g. ``("Int64", "Int32")``."""
    left_column_count: int
    right_column_count: int
    is_identical: bool

    @classmethod
    def from_schema_diff(cls, schema_diff: SchemaDiff) -> SchemaDiffSummary:
        """Build from a :class:`SchemaDiff`, stringifying its Polars dtypes."""
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
            is_identical=schema_diff.is_identical,
        )


def _json_partition(partition: Any) -> int | float | bool | str | None:
    """A partition value as a native JSON type if it has one, else its ``str()``."""
    if partition is None or isinstance(partition, int | float | bool | str):
        return partition
    return str(partition)


class PartitionRowCountChange(pydantic.BaseModel):
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


class RowCountDiffSummary(pydantic.BaseModel):
    """The JSON-report form of :class:`RowCountDiff`."""

    left_row_count: int
    right_row_count: int
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
    is_identical: bool

    @classmethod
    def from_row_count_diff(cls, row_count_diff: RowCountDiff) -> RowCountDiffSummary:
        """Build from a :class:`RowCountDiff`."""
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
            is_identical=row_count_diff.is_identical,
        )


class ParquetOutputSummary(pydantic.BaseModel):
    """The JSON-report form of a single :class:`ParquetOutput`."""

    path: str
    bytes: int
    hash: str

    @pydantic.computed_field
    @property
    def size(self) -> str:
        """:attr:`bytes` in human-readable form, e.g. ``12.3 MB``."""
        return format_bytes(self.bytes)

    @classmethod
    def from_parquet_output(cls, output: ParquetOutput) -> ParquetOutputSummary:
        """Build from a :class:`ParquetOutput`."""
        return cls(path=str(output.path), bytes=output.bytes, hash=output.hash)


#: Reasons one *section* of the JSON report's ``row_diff`` (``pk_diff`` or
#: ``non_pk_diff``) has no summary. The first three are the
#: :data:`RowComparisonSkipReason` values, for when the whole row-level comparison
#: was skipped; the last two mean that section simply doesn't apply, because the
#: table does or doesn't have a primary key.
RowDiffSectionSkipReason = Literal[
    "too_many_rows",
    "incompatible_dtypes",
    "mismatched_columns",
    "primary_key_available",
    "no_primary_key",
]


class RowDiffSectionSkipped(pydantic.BaseModel):
    """Stands in for a row diff summary that wasn't produced, and says why."""

    skipped_reason: RowDiffSectionSkipReason


class PkRowDiffSummary(pydantic.BaseModel):
    """The JSON-report form of a :class:`KeyedRowDiff`."""

    primary_key_columns: list[str]
    only_in_left_count: int
    only_in_right_count: int
    primary_keys_identical: bool
    changed_row_count: int
    """Number of shared-primary-key rows with at least one differing
    non-primary-key value."""
    column_changes: dict[str, int]
    is_identical: bool


class NonPkRowDiffSummary(pydantic.BaseModel):
    """The JSON-report form of a :class:`RowSetDiff` for a table with no primary key."""

    only_in_left_count: int
    only_in_right_count: int
    symmetric_difference_count: int
    """``only_in_left_count + only_in_right_count``. Counts rows as a
    multiset, so surplus copies of duplicated rows are included."""
    multiplicity_changed_row_count: int
    """Number of distinct rows present in both tables, but a different number
    of times."""
    is_identical: bool


class RowDiffSummary(pydantic.BaseModel):
    """The JSON-report form of :attr:`TableDiffResult.row_diff`.

    Exactly one of :attr:`pk_diff` and :attr:`non_pk_diff` is a full summary
    (unless row-level comparison was skipped entirely); the other is a
    :class:`RowDiffSectionSkipped` saying why it wasn't produced: ``no_primary_key``
    or ``primary_key_available`` when the table's primary key determined which
    kind of comparison applies, or the reason the comparison was skipped
    altogether (see :data:`RowComparisonSkipReason`), in which case the one that would
    have run carries that reason.
    """

    pk_diff: PkRowDiffSummary | RowDiffSectionSkipped
    non_pk_diff: NonPkRowDiffSummary | RowDiffSectionSkipped
    left_only_parquet: ParquetOutputSummary | None = None
    right_only_parquet: ParquetOutputSummary | None = None


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
            primary_keys_identical=row_diff.pk_diff.is_identical,
            changed_row_count=row_diff.changed_row_count,
            column_changes=row_diff.column_changes,
            is_identical=row_diff.is_identical,
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
            is_identical=row_diff.is_identical,
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


#: Version of the JSON report format written by :func:`build_pudl_diff_report`.
REPORT_SCHEMA_VERSION = "1.0.0"


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


class SizeComparison(pydantic.BaseModel):
    """The sizes of the left and right side of a comparison, and how they differ.

    Sizes are bytes on disk (or in cloud storage) of the Parquet file(s) being
    compared. Everything but the two ``*_table_bytes`` fields is derived from them
    when the report is serialized, and is ``None`` whenever either size is unknown.
    """

    left_table_bytes: int | None = None
    right_table_bytes: int | None = None

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

    Built by :func:`build_table_diff_report` from a :class:`TableDiffRun`, and
    one entry in :attr:`PudlDiffReport.tables`. Fields that describe the whole
    comparison of the two datasets (when it was run, the datasets' provenance) live
    on the :class:`PudlDiffReport` instead. Contains no row-level data itself -
    only counts and summaries; the actual differing rows are written
    separately as Parquet files (see :func:`write_row_diff_parquet`) and
    referenced from :attr:`row_diff`.
    """

    left_table_name: str
    left_table_path: str
    right_table_name: str
    right_table_path: str
    is_identical: bool
    """Conservatively ``False`` whenever :attr:`success` is ``False``, since
    a failed comparison can't establish that the tables are identical."""
    elapsed_seconds: float | None = None
    peak_rss_bytes: int | None = None
    peak_cpu_percent: float | None = None

    schema_diff: SchemaDiffSummary | None = None
    row_count_diff: RowCountDiffSummary | None = None
    row_diff: RowDiffSummary | None = None

    error: str | None = None
    """Exception message plus traceback, if the comparison failed to
    complete. ``None`` if :attr:`success` is ``True``."""
    success: bool
    """Whether the comparison completed at all, successfully or not - see
    :class:`TableDiffRun`. Distinct from :attr:`is_identical`: a
    comparison can succeed and still find the tables different."""

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
        run: The comparison's outcome, from :func:`run_table_diff`.
        left: The "left" dataset that was compared.
        right: The "right" dataset compared against it.
        table_name: Name of the table compared in ``left``.
        right_table_name: Name of the table compared in ``right``, if it
            differed from ``table_name``. Defaults to ``table_name``.
        parquet_outputs: The Parquet side-output files written for this
            table's row diff, from :func:`write_row_diff_parquet`, if any
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
            is_identical=False,
            error=run.error,
            success=False,
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
        is_identical=result.is_identical,
        elapsed_seconds=result.elapsed_seconds,
        peak_rss_bytes=result.peak_rss_bytes,
        peak_cpu_percent=result.peak_cpu_percent,
        schema_diff=SchemaDiffSummary.from_schema_diff(result.schema_diff),
        row_count_diff=RowCountDiffSummary.from_row_count_diff(result.row_count_diff),
        row_diff=row_diff_summary,
        error=None,
        success=True,
    )


class DiffOptions(pydantic.BaseModel):
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

    Never raises because the comparison failed: see :func:`run_table_diff`.

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


class DatasetInfo(DatasetProvenance):
    """One of the two compared datasets: where it is, and where it came from."""

    root: str
    """The root path or URL of the dataset's Parquet files."""


class PudlDiffSummary(SizeComparison):
    """Totals over every table in a :class:`PudlDiffReport`.

    Saves consumers from aggregating the tables themselves.

    The size fields (see :class:`SizeComparison`) total only the tables whose size
    is known on both sides.
    """

    table_count: int
    """Number of tables compared, including any whose comparison failed."""
    identical_table_count: int
    changed_table_count: int
    """Tables whose comparison completed and found differences."""
    failed_table_count: int
    """Tables whose comparison failed to complete."""
    failed_tables: list[str]
    schema_changed_tables: list[str]
    """Tables with columns added or removed, or with changed dtypes."""

    left_row_count: int
    """Total rows in the left tables, over all tables that could be counted."""
    right_row_count: int
    rows_added: int
    """Rows only in the right table, summed over tables with a row-level
    comparison."""
    rows_changed: int
    """Rows with the same primary key but changed values, summed over tables
    with a row-level comparison and a primary key."""
    rows_removed: int
    no_row_diff_table_count: int
    """Tables with no row-level comparison, whether skipped or failed. Their rows
    count towards the row totals, but not the rows added, changed or removed."""
    no_row_diff_left_row_count: int
    """Total rows in the left side of those tables."""

    columns_added: int
    columns_changed: int
    """Shared columns whose dtype changed."""
    columns_removed: int

    peak_rss_bytes: int | None = None
    """The highest :attr:`TableDiffReport.peak_rss_bytes` of any table."""
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


class PudlDiffReport(pydantic.BaseModel):
    """The full comparison of two PUDL datasets: the saved JSON report.

    Built by :func:`build_pudl_diff_report`. Holds everything that pertains to the
    comparison as a whole, plus a :class:`TableDiffReport` for each table.
    """

    schema_version: str = REPORT_SCHEMA_VERSION
    """Version of this report format, in ``major.minor.patch`` form."""
    created: str
    """UTC ISO-8601 timestamp of when this report was generated."""
    elapsed_seconds: float | None = None
    """Wall-clock time the whole comparison took."""
    left_dataset: DatasetInfo
    right_dataset: DatasetInfo
    options: DiffOptions
    tables_only_in_left: list[str]
    """Tables found only in the left dataset, which aren't compared."""
    tables_only_in_right: list[str]
    summary: PudlDiffSummary
    tables: dict[str, TableDiffReport]
    """Each compared table's report, keyed by its name in the left dataset."""

    is_identical: bool
    """Whether :attr:`success` is ``True`` and every compared table is identical.
    Tables found in only one dataset don't count against this."""
    error: str | None = None
    """Why the comparison as a whole failed, e.g. no tables could be listed. This
    is ``None`` when the only failures are of individual tables, which each
    record their own :attr:`TableDiffReport.error`."""
    success: bool
    """Whether the comparison completed: :attr:`error` is ``None`` and so is
    every table's. Distinct from :attr:`is_identical`: a comparison can succeed
    and still find differences."""

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
        return DatasetInfo(root=str(dataset.root), **provenance.model_dump())

    success = error is None and all(report.success for report in tables.values())
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
        is_identical=success and all(r.is_identical for r in tables.values()),
        error=error,
        success=success,
    )
