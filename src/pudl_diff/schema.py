"""Comparing the schemas of two tables."""

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class SchemaDiff:
    """The difference between two table schemas.

    Column order is not considered significant, per PUDL Diff's definition of
    "functionally identical" tables.
    """

    columns_only_in_left: list[str]
    columns_only_in_right: list[str]
    dtype_changes: dict[str, tuple[pl.DataType, pl.DataType]]
    """Maps column name to a ``(left_dtype, right_dtype)`` pair, for columns
    present in both schemas but with differing dtypes."""
    left_column_count: int
    """Total number of columns in the left schema."""
    right_column_count: int
    """Total number of columns in the right schema."""

    @property
    def is_identical(self) -> bool:
        """Whether the two schemas have the same columns and dtypes."""
        return not (
            self.columns_only_in_left
            or self.columns_only_in_right
            or self.dtype_changes
        )


def compare_schemas(left: pl.Schema, right: pl.Schema) -> SchemaDiff:
    """Compare two table schemas.

    Args:
        left: Schema of the "left" table, e.g. from
            ``PudlDiffDataset.scan_table(table_name).collect_schema()``.
        right: Schema of the "right" table, to compare against ``left``.
    """
    left_columns = set(left)
    right_columns = set(right)
    shared_columns = left_columns & right_columns
    dtype_changes = {
        name: (left[name], right[name])
        for name in shared_columns
        if left[name] != right[name]
    }
    return SchemaDiff(
        columns_only_in_left=sorted(left_columns - right_columns),
        columns_only_in_right=sorted(right_columns - left_columns),
        dtype_changes=dtype_changes,
        left_column_count=len(left),
        right_column_count=len(right),
    )
