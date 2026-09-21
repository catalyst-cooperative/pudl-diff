"""Comparing a single table between two datasets."""

import dataclasses
import logging
import time
import traceback
from dataclasses import dataclass
from typing import Literal

import polars as pl

from pudl_diff.dataset import PudlDiffDataset
from pudl_diff.logs import get_logger
from pudl_diff.performance import PerformanceSampler
from pudl_diff.row_counts import RowCountDiff, compare_row_counts
from pudl_diff.rows import (
    KeyedRowDiff,
    RowSetDiff,
    compare_rows_with_pk,
    compare_rows_without_pk,
)
from pudl_diff.schema import SchemaDiff, compare_schemas

logger: logging.Logger = get_logger(__name__)


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
    thread (see :class:`~.PerformanceSampler`), so very short, sharp spikes
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
    rtol: float = 1e-5,
    atol: float = 1e-8,
    max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
) -> TableDiffResult:
    """Compare a single table between two PUDL datasets.

    Times the comparison and samples this process's peak RSS and CPU
    utilization while it runs (see :class:`~.PerformanceSampler`), recording
    all three on the returned :class:`TableDiffResult`. See
    :func:`_compare_table` for the comparison logic itself.
    """
    start = time.perf_counter()
    with PerformanceSampler() as sampler:
        result = _compare_table(
            left,
            right,
            table_name,
            right_table_name=right_table_name,
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
    rtol: float = 1e-5,
    atol: float = 1e-8,
    max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
) -> TableDiffResult:
    """Compare a single table between two PUDL datasets.

    Runs the schema and row-count comparisons, then dispatches to
    :func:`~.compare_rows_with_pk` or :func:`~.compare_rows_without_pk` depending on
    whether ``table_name`` has a primary key (per ``left``'s datapackage).

    Args:
        left: The "left" dataset to compare.
        right: The "right" dataset to compare against ``left``.
        table_name: Name of the table to compare in ``left``, and in ``right``
            too unless ``right_table_name`` is given.
        right_table_name: Name of the table to compare in ``right``, if it
            differs from ``table_name`` - e.g. comparing a `core_` table
            against the `out_` table built from it. Defaults to ``table_name``.
            The primary key is still looked up under ``table_name``, so this
            assumes ``right_table_name``'s schema is compatible enough to
            share it (e.g. sharing the same primary key columns).
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

    row_count_diff = compare_row_counts(left_lf, right_lf)

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
            rtol=rtol,
            atol=atol,
            max_rows_for_row_level_comparison=max_rows_for_row_level_comparison,
        )
    except Exception:
        logger.exception(f"Comparison of {table_name!r} failed.")
        return TableDiffRun(success=False, result=None, error=traceback.format_exc())
    return TableDiffRun(success=True, result=result, error=None)


def row_diff_left_right_frames(
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
