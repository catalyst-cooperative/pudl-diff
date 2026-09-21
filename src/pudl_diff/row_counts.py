"""Comparing the row counts of two tables."""

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class RowCountDiff:
    """The row counts of two tables."""

    left_row_count: int
    """Row count of the left table."""
    right_row_count: int
    """Row count of the right table."""

    @property
    def is_identical(self) -> bool:
        """Whether the tables have the same number of rows."""
        return self.left_row_count == self.right_row_count


def count_rows(lf: pl.LazyFrame) -> int:
    """Count the rows of `lf`, streaming so that it needn't fit in memory."""
    return lf.select(pl.len()).collect(engine="streaming").item()


def compare_row_counts(left: pl.LazyFrame, right: pl.LazyFrame) -> RowCountDiff:
    """Compare the row counts of two tables.

    Args:
        left: The "left" table to compare.
        right: The "right" table to compare against `left`.
    """
    return RowCountDiff(
        left_row_count=count_rows(left), right_row_count=count_rows(right)
    )
