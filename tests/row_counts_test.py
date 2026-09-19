"""Unit tests for pudl.validate.diff.row_counts."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest
import yaml

from pudl.validate.diff.row_counts import (
    NO_PARTITION,
    compare_row_counts,
    dbt_partition_expr_to_polars,
    get_dbt_partition_expr,
    get_partition_expr_for_table,
)


def test_compare_row_counts_no_partition_identical():
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [4, 5, 6]})
    result = compare_row_counts(left, right)
    assert result.is_identical
    assert result.changes == {}
    assert result.left_row_count == 3
    assert result.right_row_count == 3


def test_compare_row_counts_no_partition_mismatch():
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [4, 5]})
    result = compare_row_counts(left, right)
    assert not result.is_identical
    assert result.changes == {NO_PARTITION: (3, 2)}
    assert result.left_row_count == 3
    assert result.right_row_count == 2


def test_compare_row_counts_by_partition_identical():
    left = pl.LazyFrame({"year": [2020, 2020, 2021]})
    right = pl.LazyFrame({"year": [2021, 2020, 2020]})
    result = compare_row_counts(left, right, partition_expr="year")
    assert result.is_identical
    assert result.changes == {}
    assert result.left_row_count == 3
    assert result.right_row_count == 3


def test_compare_row_counts_by_partition_mismatch():
    left = pl.LazyFrame({"year": [2020, 2020, 2021]})
    right = pl.LazyFrame({"year": [2020, 2021, 2021]})
    result = compare_row_counts(left, right, partition_expr="year")
    assert not result.is_identical
    assert result.changes == {2020: (2, 1), 2021: (1, 2)}
    assert result.left_row_count == 3
    assert result.right_row_count == 3


def test_compare_row_counts_extra_and_missing_partition():
    left = pl.LazyFrame({"year": [2020, 2021]})
    right = pl.LazyFrame({"year": [2020, 2022]})
    result = compare_row_counts(left, right, partition_expr="year")
    assert result.changes == {2021: (1, None), 2022: (None, 1)}
    assert result.left_row_count == 2
    assert result.right_row_count == 2


def test_compare_row_counts_null_partition_value():
    left = pl.LazyFrame({"year": [2020, None]})
    right = pl.LazyFrame({"year": [2020]})
    result = compare_row_counts(left, right, partition_expr="year")
    assert result.changes == {None: (1, None)}


def test_compare_row_counts_with_expression_partition():
    left = pl.LazyFrame({"report_date": [date(2020, 1, 1), date(2021, 1, 1)]})
    right = pl.LazyFrame({"report_date": [date(2020, 1, 1)]})
    result = compare_row_counts(
        left, right, partition_expr=pl.col("report_date").dt.year()
    )
    assert result.changes == {2021: (1, None)}


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("report_year", pl.col("report_year")),
        ("EXTRACT(YEAR FROM report_date)", pl.col("report_date").dt.year()),
        ("extract(year from report_date)", pl.col("report_date").dt.year()),
        ("EXTRACT(YEAR FROM datetime_utc)", pl.col("datetime_utc").dt.year()),
    ],
)
def test_dbt_partition_expr_to_polars(expr: str, expected: pl.Expr):
    assert dbt_partition_expr_to_polars(expr).meta.eq(expected)


def test_dbt_partition_expr_to_polars_unsupported():
    with pytest.raises(ValueError, match="Don't know how to translate"):
        dbt_partition_expr_to_polars("EXTRACT(MONTH FROM report_date)")


def _write_dbt_schema_yml(
    dbt_path: Path, source: str, table_name: str, data_tests: list
) -> None:
    table_dir = dbt_path / "models" / source / table_name
    table_dir.mkdir(parents=True)
    descriptor = {
        "version": 2,
        "sources": [
            {
                "name": "pudl",
                "tables": [{"name": table_name, "data_tests": data_tests}],
            }
        ],
    }
    (table_dir / "schema.yml").write_text(yaml.dump(descriptor))


def test_get_dbt_partition_expr(tmp_path: Path):
    _write_dbt_schema_yml(
        tmp_path,
        "ferc1",
        "core_ferc1__yearly_test_table",
        [
            "expect_columns_not_all_null",
            {
                "check_row_counts_per_partition": {
                    "arguments": {"partition_expr": "report_year"}
                }
            },
        ],
    )
    assert (
        get_dbt_partition_expr("core_ferc1__yearly_test_table", dbt_path=tmp_path)
        == "report_year"
    )


def test_get_dbt_partition_expr_no_test_configured(tmp_path: Path):
    _write_dbt_schema_yml(
        tmp_path, "ferc", "core_ferc__entity_companies", ["expect_columns_not_all_null"]
    )
    assert (
        get_dbt_partition_expr("core_ferc__entity_companies", dbt_path=tmp_path) is None
    )


def test_get_dbt_partition_expr_no_schema_yml(tmp_path: Path):
    assert get_dbt_partition_expr("nonexistent_table", dbt_path=tmp_path) is None


def test_get_partition_expr_for_table(tmp_path: Path):
    _write_dbt_schema_yml(
        tmp_path,
        "eia930",
        "core_eia930__hourly_operations",
        [
            {
                "check_row_counts_per_partition": {
                    "arguments": {"partition_expr": "EXTRACT(YEAR FROM datetime_utc)"}
                }
            }
        ],
    )
    result = get_partition_expr_for_table(
        "core_eia930__hourly_operations", dbt_path=tmp_path
    )
    assert result is not None
    assert result.meta.eq(pl.col("datetime_utc").dt.year())


def test_get_partition_expr_for_table_no_test_configured(tmp_path: Path):
    _write_dbt_schema_yml(
        tmp_path, "ferc", "core_ferc__entity_companies", ["expect_columns_not_all_null"]
    )
    assert (
        get_partition_expr_for_table("core_ferc__entity_companies", dbt_path=tmp_path)
        is None
    )


def test_get_dbt_partition_expr_against_real_repo():
    """Sanity check against the actual PUDL repo, using its default dbt_path."""
    assert (
        get_dbt_partition_expr(
            "core_ferc1__yearly_other_regulatory_liabilities_sched278"
        )
        == "report_year"
    )
    assert get_dbt_partition_expr("core_ferc__entity_companies") is None
