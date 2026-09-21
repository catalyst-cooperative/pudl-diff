"""Unit tests for pudl_diff.schema."""

import polars as pl

from pudl_diff import table_report
from pudl_diff.schema import compare_schemas


def test_compare_schemas_identical():
    """Compare schemas identical."""
    schema = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    result = compare_schemas(schema, schema)
    assert result.is_identical
    assert result.columns_only_in_left == []
    assert result.columns_only_in_right == []
    assert result.dtype_changes == {}


def test_compare_schemas_column_order_independent():
    """The order of the columns doesn't matter, only which columns there are."""
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"y": pl.String(), "x": pl.Int64()})
    assert compare_schemas(left, right).is_identical


def test_compare_schemas_extra_and_missing_columns():
    """Compare schemas extra and missing columns."""
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"x": pl.Int64(), "z": pl.Float64()})
    result = compare_schemas(left, right)
    assert not result.is_identical
    assert result.columns_only_in_left == ["y"]
    assert result.columns_only_in_right == ["z"]
    assert result.dtype_changes == {}


def test_compare_schemas_column_counts():
    """Compare schemas column counts."""
    left = pl.Schema({"x": pl.Int64(), "y": pl.String(), "w": pl.Int64()})
    right = pl.Schema({"x": pl.Int64(), "z": pl.Float64()})
    result = compare_schemas(left, right)
    assert result.left_column_count == 3
    assert result.right_column_count == 2
    summary = table_report.SchemaDiffSummary.from_schema_diff(result)
    assert summary.left_column_count == 3
    assert summary.right_column_count == 2


def test_compare_schemas_dtype_mismatch():
    """A column whose dtype differs is a dtype change, and not a removed and an added column.

    The change records the dtype on each side.
    """
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"x": pl.Int32(), "y": pl.String()})
    result = compare_schemas(left, right)
    assert not result.is_identical
    assert result.columns_only_in_left == []
    assert result.columns_only_in_right == []
    assert result.dtype_changes == {"x": (pl.Int64(), pl.Int32())}
