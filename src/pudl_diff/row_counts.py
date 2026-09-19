"""Comparing the row counts of two tables, overall and per partition."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from pudl import PUDL_DBT_PATH

#: Sentinel partition key used in :attr:`RowCountDiff.changes` for the overall
#: row count when :func:`compare_row_counts` is called without a ``partition_expr``.
#: Distinct from any real partition value, including a null one, since those only
#: ever appear together with an actual ``partition_expr``.
NO_PARTITION: object = object()


@dataclass(frozen=True)
class RowCountDiff:
    """Per-partition row count differences between two tables.

    Only partitions whose row counts differ are included in :attr:`changes`,
    mirroring the ``check_row_counts_per_partition`` dbt macro this reimplements
    in Polars.
    """

    partition_expr: str | pl.Expr | None
    changes: dict[Any, tuple[int | None, int | None]]
    """Maps partition value to a ``(left_row_count, right_row_count)`` pair.
    Either is ``None`` if that partition is missing entirely from one side. When
    no ``partition_expr`` was given, the sole key represents the overall row
    count."""
    left_row_count: int
    """Total row count of the left table, across all partitions."""
    right_row_count: int
    """Total row count of the right table, across all partitions."""
    partition_label: str | None = None
    """How to describe :attr:`partition_expr` in reports, if not by ``str()`` of
    it: e.g. the dbt SQL expression it was translated from."""

    @property
    def is_identical(self) -> bool:
        """Whether every partition has the same row count on both sides."""
        return not self.changes


_PARTITION_COL_NAME = "__partition__"


def _count_rows(lf: pl.LazyFrame) -> int:
    return lf.select(pl.len()).collect(engine="streaming").item()


def _partition_counts(
    lf: pl.LazyFrame, partition_expr: str | pl.Expr | None
) -> dict[Any, int]:
    if partition_expr is None:
        return {NO_PARTITION: _count_rows(lf)}
    expr = pl.col(partition_expr) if isinstance(partition_expr, str) else partition_expr
    counts = (
        lf.group_by(expr.alias(_PARTITION_COL_NAME))
        .agg(pl.len().alias("__count__"))
        .collect(engine="streaming")
    )
    return dict(
        zip(
            counts[_PARTITION_COL_NAME].to_list(),
            counts["__count__"].to_list(),
            strict=True,
        )
    )


def compare_row_counts(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    partition_expr: str | pl.Expr | None = None,
    partition_label: str | None = None,
) -> RowCountDiff:
    """Compare row counts between two tables, optionally grouped by partition.

    Args:
        left: The "left" table to compare.
        right: The "right" table to compare against ``left``.
        partition_expr: Column name or Polars expression to group row counts by
            (e.g. ``"report_year"`` or ``pl.col("report_date").dt.year()``)
            before comparing, so that a change confined to one partition
            doesn't get masked by the overall row count staying the same. If
            not given, compares the total row count of each table. See
            :func:`get_partition_expr_for_table` to derive this automatically
            from PUDL's dbt row-count test configuration.
        partition_label: How to describe ``partition_expr`` in reports. Defaults
            to ``str()`` of it; pass the dbt SQL expression it was translated
            from to make the report traceable back to the dbt schema files.
    """
    left_counts = _partition_counts(left, partition_expr)
    right_counts = _partition_counts(right, partition_expr)
    all_partitions = set(left_counts) | set(right_counts)
    changes = {
        partition: (left_counts.get(partition), right_counts.get(partition))
        for partition in all_partitions
        if left_counts.get(partition) != right_counts.get(partition)
    }
    return RowCountDiff(
        partition_expr=partition_expr,
        changes=changes,
        left_row_count=sum(left_counts.values()),
        right_row_count=sum(right_counts.values()),
        partition_label=partition_label,
    )


# dbt's check_row_counts_per_partition test only ever configures partition_expr as
# either a bare column name or EXTRACT(YEAR FROM <column>) (case-insensitive) across
# every PUDL model - no freeform SQL - so those are the only two forms we translate.
_EXTRACT_YEAR_RE = re.compile(r"(?i)^EXTRACT\(\s*YEAR\s+FROM\s+(\w+)\s*\)$")
_BARE_COLUMN_RE = re.compile(r"^\w+$")


def get_dbt_partition_expr(
    table_name: str, dbt_path: Path = PUDL_DBT_PATH
) -> str | None:
    """Look up the ``partition_expr`` dbt uses to test row counts for a table.

    Reads the built ``dbt/models/<source>/<table_name>/schema.yml`` file (not the
    hand-edited ``dbt/schema_inputs/`` templates dbt generates it from), so the
    result always matches what dbt itself uses for
    ``check_row_counts_per_partition``.

    Args:
        table_name: Name of the table to look up.
        dbt_path: Path to the local ``dbt/`` directory. Defaults to
            :data:`pudl.PUDL_DBT_PATH`, i.e. the ``dbt/`` directory of the PUDL
            repository this code is running from.

    Returns:
        The raw ``partition_expr`` string, or ``None`` if the table has no
        ``check_row_counts_per_partition`` test configured.
    """
    matches = list(dbt_path.glob(f"models/*/{table_name}/schema.yml"))
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"Found multiple dbt schema.yml files for table {table_name!r}: {matches}"
        )
    descriptor = yaml.safe_load(matches[0].read_text())
    (table,) = descriptor["sources"][0]["tables"]
    for test in table.get("data_tests", []):
        if isinstance(test, dict) and "check_row_counts_per_partition" in test:
            return test["check_row_counts_per_partition"]["arguments"]["partition_expr"]
    return None


def dbt_partition_expr_to_polars(expr: str) -> pl.Expr:
    """Translate a dbt ``partition_expr`` string into an equivalent Polars expression.

    Supports the two forms used across PUDL's dbt models: a bare column name, and
    ``EXTRACT(YEAR FROM <column>)`` (case-insensitive).

    Raises:
        ValueError: if ``expr`` doesn't match either supported form, since we
            can't reliably translate arbitrary SQL into a Polars expression.
    """
    expr = expr.strip()
    if match := _EXTRACT_YEAR_RE.match(expr):
        return pl.col(match.group(1)).dt.year()
    if _BARE_COLUMN_RE.match(expr):
        return pl.col(expr)
    raise ValueError(
        f"Don't know how to translate dbt partition_expr {expr!r} to a Polars "
        "expression."
    )


def get_partition_expr_for_table(
    table_name: str, dbt_path: Path = PUDL_DBT_PATH
) -> pl.Expr | None:
    """The Polars partition expression to use when comparing a table's row counts.

    Combines :func:`get_dbt_partition_expr` and :func:`dbt_partition_expr_to_polars`
    so callers get an expression ready to pass to :func:`compare_row_counts`,
    guaranteed to match the partitioning dbt itself uses for this table.

    Returns:
        The translated Polars expression, or ``None`` if the table has no
        ``check_row_counts_per_partition`` test configured.
    """
    partition_expr = get_dbt_partition_expr(table_name, dbt_path=dbt_path)
    if partition_expr is None:
        return None
    return dbt_partition_expr_to_polars(partition_expr)
