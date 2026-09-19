"""Unit tests for pudl.validate.diff.table."""

from pathlib import Path

import polars as pl
import pytest

from pudl.validate.diff import table as diff_table
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.row_counts import (
    NO_PARTITION,
)
from pudl.validate.diff.rows import (
    KeyedRowDiff,
    RowSetDiff,
)
from pudl.validate.diff.table import TableDiffResult, compare_table, run_table_diff


def test_run_table_diff_succeeds_without_left_datapackage(tmp_path: Path, make_dataset):
    """The local-dev scenario this fix targets: no datapackage.json at all.

    Uses a real PUDL table name (with a real primary key column) so the
    fallback to ``pudl.metadata.classes.PUDL_PACKAGE`` in ``primary_key()``
    has something to find, letting the comparison run end to end even
    though the left dataset can't describe itself at all.
    """
    table_name = "core_eia__codes_wet_dry_bottom"
    left_root = tmp_path / "left"
    left_root.mkdir()
    pl.DataFrame(
        {"code": ["a", "b"], "label": ["A", "B"], "description": ["d1", "d2"]}
    ).write_parquet(left_root / f"{table_name}.parquet")
    left = PudlDiffDataset(left_root)

    right_resource = {
        "name": table_name,
        "schema": {
            "fields": [
                {"name": "code", "type": "string"},
                {"name": "label", "type": "string"},
                {"name": "description", "type": "string"},
            ],
            "primaryKey": ["code"],
        },
    }
    right = make_dataset(
        tmp_path / "right",
        [right_resource],
        {
            table_name: pl.DataFrame(
                {"code": ["a", "b"], "label": ["A", "B"], "description": ["d1", "d2"]}
            )
        },
    )

    run = run_table_diff(left, right, table_name, auto_partition=False)
    assert run.success
    assert run.result is not None
    assert isinstance(run.result.row_diff, KeyedRowDiff)
    assert run.result.is_identical


def test_compare_table_records_elapsed_time_and_performance_stats(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert result.elapsed_seconds > 0
    assert result.peak_rss_bytes >= 0
    assert result.peak_cpu_percent >= 0.0


def test_compare_table_identical_with_pk(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [2, 1], "y": ["b", "a"]})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert result.is_identical
    assert result.table_name == "table_with_pk"
    assert result.right_table_name == "table_with_pk"
    assert result.schema_diff.is_identical
    assert result.row_count_diff.is_identical
    assert isinstance(result.row_diff, KeyedRowDiff)
    assert result.row_diff.is_identical


def test_compare_table_incompatible_dtypes_skip_reason(
    tmp_path: Path, mocker, pk_resource, make_dataset
):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [2, 1], "y": ["b", "a"]})},
    )
    mocker.patch.object(
        diff_table,
        "compare_rows_with_pk",
        side_effect=pl.exceptions.PolarsError("boom"),
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert result.row_diff is None
    assert result.row_diff_skipped_reason == "incompatible_dtypes"
    assert not result.is_identical


def test_compare_table_mismatched_key_dtypes_skip_row_diff(
    tmp_path: Path, pk_resource, make_dataset
):
    """Key hashes of different dtypes differ silently, so this must be caught."""
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int32)})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert result.row_diff is None
    assert result.row_diff_skipped_reason == "incompatible_dtypes"


def test_compare_table_different_right_table_name(
    tmp_path: Path, pk_resource, make_dataset
):
    left_resources = [pk_resource("core_table", ["x"])]
    right_resources = [
        {
            "name": "out_table",
            "schema": {
                "fields": [
                    {"name": "x", "type": "integer"},
                    {"name": "y", "type": "string"},
                    {"name": "z", "type": "integer"},
                ],
                "primaryKey": ["x"],
            },
        }
    ]
    left = make_dataset(
        tmp_path / "left",
        left_resources,
        {"core_table": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {"out_table": pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "z": [10, 20]})},
    )
    result = compare_table(
        left, right, "core_table", right_table_name="out_table", auto_partition=False
    )
    assert result.table_name == "core_table"
    assert result.right_table_name == "out_table"
    assert not result.schema_diff.is_identical
    assert result.schema_diff.columns_only_in_right == ["z"]
    assert result.row_count_diff.is_identical
    assert isinstance(result.row_diff, KeyedRowDiff)
    # shared columns (x, y) match even though the tables aren't identical overall
    assert result.row_diff.is_identical
    assert not result.is_identical


def test_compare_table_differing_with_pk(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "changed"]})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert not result.is_identical
    assert result.row_count_diff.is_identical
    assert isinstance(result.row_diff, KeyedRowDiff)
    assert result.row_diff.column_changes == {"y": 1}


def test_compare_table_identical_without_pk(
    tmp_path: Path, no_pk_resource, make_dataset
):
    resources = [no_pk_resource("table_without_pk")]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [2, 1], "y": ["b", "a"]})},
    )
    result = compare_table(left, right, "table_without_pk", auto_partition=False)
    assert result.is_identical
    assert isinstance(result.row_diff, RowSetDiff)


def test_compare_table_differing_without_pk(
    tmp_path: Path, no_pk_resource, make_dataset
):
    resources = [no_pk_resource("table_without_pk")]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 3], "y": ["a", "c"]})},
    )
    result = compare_table(left, right, "table_without_pk", auto_partition=False)
    assert not result.is_identical
    assert isinstance(result.row_diff, RowSetDiff)
    assert not result.row_diff.is_identical


def test_compare_table_differing_schema_with_pk_falls_back_to_shared_columns(
    tmp_path: Path,
    make_dataset,
):
    left_resources = [
        {
            "name": "table_with_pk",
            "schema": {
                "fields": [
                    {"name": "x", "type": "integer"},
                    {"name": "y", "type": "string"},
                    {"name": "w", "type": "integer"},
                ],
                "primaryKey": ["x"],
            },
        }
    ]
    right_resources = [
        {
            "name": "table_with_pk",
            "schema": {
                "fields": [
                    {"name": "x", "type": "integer"},
                    {"name": "y", "type": "string"},
                    {"name": "z", "type": "integer"},
                ],
                "primaryKey": ["x"],
            },
        }
    ]
    left = make_dataset(
        tmp_path / "left",
        left_resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "w": [10, 20]})},
    )
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {
            "table_with_pk": pl.DataFrame(
                {"x": [1, 2], "y": ["a", "changed"], "z": [100, 200]}
            )
        },
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert not result.is_identical
    assert not result.schema_diff.is_identical
    assert isinstance(result.row_diff, KeyedRowDiff)
    assert result.row_diff.pk_diff.is_identical
    assert result.row_diff.column_changes == {"y": 1}


def test_compare_table_differing_schema_missing_pk_column(
    tmp_path: Path, pk_resource, make_dataset
):
    left_resources = [pk_resource("table_with_pk", ["x"])]
    right_resources = [
        {
            "name": "table_with_pk",
            "schema": {
                "fields": [{"name": "z", "type": "integer"}],
                "primaryKey": ["z"],
            },
        }
    ]
    left = make_dataset(
        tmp_path / "left",
        left_resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {"table_with_pk": pl.DataFrame({"z": [1, 2]})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert not result.is_identical
    assert not result.schema_diff.is_identical
    assert result.row_diff is None
    assert result.row_diff_skipped_reason == "mismatched_columns"


def test_compare_table_differing_schema_without_pk_skips_row_diff(
    tmp_path: Path, no_pk_resource, make_dataset
):
    left_resources = [no_pk_resource("table_without_pk")]
    right_resources = [
        {
            "name": "table_without_pk",
            "schema": {
                "fields": [
                    {"name": "x", "type": "integer"},
                    {"name": "z", "type": "integer"},
                ],
            },
        }
    ]
    left = make_dataset(
        tmp_path / "left",
        left_resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "z": [1, 2]})},
    )
    result = compare_table(left, right, "table_without_pk", auto_partition=False)
    assert not result.is_identical
    assert not result.schema_diff.is_identical
    assert result.row_diff is None
    assert result.row_diff_skipped_reason == "mismatched_columns"


def test_compare_table_auto_partition_no_dbt_config(
    tmp_path: Path, no_pk_resource, make_dataset
):
    """A table with no dbt row-count test falls back to a whole-table count."""
    resources = [no_pk_resource("table_without_pk")]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1], "y": ["a"]})},
    )
    result = compare_table(left, right, "table_without_pk")
    assert result.row_count_diff.changes == {NO_PARTITION: (2, 1)}


def test_compare_table_explicit_partition_expr(
    tmp_path: Path, no_pk_resource, make_dataset
):
    resources = [no_pk_resource("table_without_pk")]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [2020, 2020, 2021], "y": [1, 2, 3]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [2020, 2021, 2021], "y": [1, 2, 3]})},
    )
    result = compare_table(left, right, "table_without_pk", partition_expr="x")
    assert result.row_count_diff.changes == {2020: (2, 1), 2021: (1, 2)}


def test_compare_table_skips_row_diff_above_max_rows(
    tmp_path, pk_resource, make_dataset
):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    result = compare_table(
        left,
        right,
        "table_with_pk",
        auto_partition=False,
        max_rows_for_row_level_comparison=1,
    )
    assert result.row_diff is None
    assert result.schema_diff.is_identical
    assert result.row_count_diff.is_identical
    assert result.row_diff_skipped_reason == "too_many_rows"
    # is_identical is conservatively False since row content was never checked,
    # even though schema and row counts alone match.
    assert not result.is_identical


def test_compare_table_default_max_rows_for_row_level_comparison(
    tmp_path, pk_resource, make_dataset
):
    """The module constant is still used as the default when not overridden."""
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    result = compare_table(left, right, "table_with_pk", auto_partition=False)
    assert result.row_diff is not None
    assert result.row_diff_skipped_reason is None


def test_run_table_diff_success(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
    assert run.success
    assert run.error is None
    assert isinstance(run.result, TableDiffResult)
    assert run.result.is_identical


def test_run_table_diff_missing_datapackage(tmp_path: Path, pk_resource, make_dataset):
    """A dataset whose datapackage.json can't even be read yields a failed run."""
    left_root = tmp_path / "left"
    left_root.mkdir()
    left = PudlDiffDataset(left_root)

    right_resources = [pk_resource("table_with_pk", ["x"])]
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )

    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
    assert not run.success
    assert run.result is None
    assert run.error is not None
    assert "FileNotFoundError" in run.error


def test_run_table_diff_unknown_table(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    run = run_table_diff(left, right, "nonexistent_table")
    assert not run.success
    assert run.result is None
    assert run.error is not None
    # table_path() no longer validates against the datapackage (see
    # test_run_table_diff_succeeds_without_left_datapackage), so the error
    # now comes from Polars failing to find the file itself.
    assert "FileNotFoundError" in run.error
    assert "nonexistent_table.parquet" in run.error


@pytest.mark.parametrize("has_pk", [True, False])
def test_compare_table_ignores_row_and_column_order(
    has_pk: bool, tmp_path: Path, pk_resource, no_pk_resource, make_dataset
):
    resource = pk_resource if has_pk else no_pk_resource
    resources = [resource("some_table", ["x"]) if has_pk else resource("some_table")]
    left_df = pl.DataFrame({"x": [1, 2, 3, 4], "y": ["a", "b", "c", "d"]})
    right_df = left_df.reverse().select("y", "x")
    left = make_dataset(tmp_path / "left", resources, {"some_table": left_df})
    right = make_dataset(tmp_path / "right", resources, {"some_table": right_df})

    result = compare_table(left, right, "some_table", auto_partition=False)

    assert result.is_identical
    assert result.schema_diff.is_identical
    assert result.row_count_diff.is_identical
    assert result.row_diff is not None
    assert result.row_diff.is_identical
