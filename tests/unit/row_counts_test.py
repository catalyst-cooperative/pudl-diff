"""Unit tests for pudl_diff.row_counts."""

import polars as pl

from pudl_diff.row_counts import compare_row_counts, count_rows


def test_count_rows():
    """Count rows."""
    assert count_rows(pl.LazyFrame({"x": [1, 2, 3]})) == 3


def test_compare_row_counts_identical():
    """Compare row counts identical."""
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [4, 5, 6]})
    result = compare_row_counts(left, right)
    assert result.is_identical
    assert result.left_row_count == 3
    assert result.right_row_count == 3


def test_compare_row_counts_mismatch():
    """Compare row counts mismatch."""
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [4, 5]})
    result = compare_row_counts(left, right)
    assert not result.is_identical
    assert result.left_row_count == 3
    assert result.right_row_count == 2
