"""Unit tests for pudl.validate.diff.schema."""

import polars as pl

from pudl.validate.diff import table_report
from pudl.validate.diff.schema import compare_schemas


def test_compare_schemas_identical():
    schema = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    result = compare_schemas(schema, schema)
    assert result.is_identical
    assert result.columns_only_in_left == []
    assert result.columns_only_in_right == []
    assert result.dtype_changes == {}


def test_compare_schemas_column_order_independent():
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"y": pl.String(), "x": pl.Int64()})
    assert compare_schemas(left, right).is_identical


def test_compare_schemas_extra_and_missing_columns():
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"x": pl.Int64(), "z": pl.Float64()})
    result = compare_schemas(left, right)
    assert not result.is_identical
    assert result.columns_only_in_left == ["y"]
    assert result.columns_only_in_right == ["z"]
    assert result.dtype_changes == {}


def test_compare_schemas_column_counts():
    left = pl.Schema({"x": pl.Int64(), "y": pl.String(), "w": pl.Int64()})
    right = pl.Schema({"x": pl.Int64(), "z": pl.Float64()})
    result = compare_schemas(left, right)
    assert result.left_column_count == 3
    assert result.right_column_count == 2
    summary = table_report.SchemaDiffSummary.from_schema_diff(result)
    assert summary.left_column_count == 3
    assert summary.right_column_count == 2


def test_compare_schemas_dtype_mismatch():
    left = pl.Schema({"x": pl.Int64(), "y": pl.String()})
    right = pl.Schema({"x": pl.Int32(), "y": pl.String()})
    result = compare_schemas(left, right)
    assert not result.is_identical
    assert result.columns_only_in_left == []
    assert result.columns_only_in_right == []
    assert result.dtype_changes == {"x": (pl.Int64(), pl.Int32())}
