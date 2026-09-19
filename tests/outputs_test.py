"""Unit tests for pudl.validate.diff.outputs."""

import hashlib
from pathlib import Path

import polars as pl

from pudl.validate.diff.outputs import write_row_diff_parquet
from pudl.validate.diff.rows import (
    compare_rows_with_pk,
    compare_rows_without_pk,
)


def test_write_row_diff_parquet_pk_table(tmp_path: Path):
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1, 2, 3]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2, 99, 4]})
    row_diff = compare_rows_with_pk(left, right, ["id"])

    outputs = write_row_diff_parquet(row_diff, tmp_path / "out", "my_table")

    assert outputs is not None
    assert outputs.left.path.name == "my_table_left_only.parquet"
    assert outputs.right.path.name == "my_table_right_only.parquet"
    left_df = pl.read_parquet(outputs.left.path)
    right_df = pl.read_parquet(outputs.right.path)
    # id 1 is left-only (pk symmetric diff); id 4 is right-only; id 3's val
    # differs between the two, so both sides' versions of it are included.
    assert sorted(left_df["id"].to_list()) == [1, 3]
    assert sorted(right_df["id"].to_list()) == [3, 4]
    assert left_df.schema == left.collect_schema()
    assert right_df.schema == right.collect_schema()


def test_write_row_diff_parquet_differing_table_names(tmp_path: Path):
    """core_*/out_* comparisons use different names for the left and right tables."""
    left = pl.LazyFrame({"id": [1, 2], "val": [1, 2]})
    right = pl.LazyFrame({"id": [1, 2], "val": [1, 2], "extra": [10, 20]})
    row_diff = compare_rows_with_pk(left, right, ["id"])

    outputs = write_row_diff_parquet(
        row_diff,
        tmp_path / "out",
        "core_table",
        right_table_name="out_table",
    )

    assert outputs is not None
    assert outputs.left.path.name == "core_table_left_only.parquet"
    assert outputs.right.path.name == "out_table_right_only.parquet"


def test_write_row_diff_parquet_keyed_combines_one_sided_and_mismatched(
    tmp_path: Path,
):
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1, 2, 3]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2, 30, 4]})
    row_diff = compare_rows_with_pk(left, right, ["id"])

    outputs = write_row_diff_parquet(row_diff, tmp_path / "out", "my_table")

    assert outputs is not None
    assert outputs.left.total_row_count == 2
    assert outputs.right.total_row_count == 2
    assert sorted(pl.read_parquet(outputs.left.path)["id"].to_list()) == [1, 3]
    assert sorted(pl.read_parquet(outputs.right.path)["id"].to_list()) == [3, 4]


def test_write_row_diff_parquet_without_pk_table(tmp_path: Path):
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [2, 3, 4]})
    row_diff = compare_rows_without_pk(left, right)

    outputs = write_row_diff_parquet(row_diff, tmp_path / "out", "my_table")

    assert outputs is not None
    left_df = pl.read_parquet(outputs.left.path)
    right_df = pl.read_parquet(outputs.right.path)
    assert left_df["x"].to_list() == [1]
    assert right_df["x"].to_list() == [4]
    assert left_df.schema == left.collect_schema()
    assert right_df.schema == right.collect_schema()


def test_write_row_diff_parquet_row_cap(tmp_path: Path):
    left = pl.LazyFrame({"x": [1, 2, 3, 4, 5]})
    right = pl.LazyFrame({"x": []}, schema={"x": pl.Int64})
    row_diff = compare_rows_without_pk(left, right)

    outputs = write_row_diff_parquet(
        row_diff, tmp_path / "out", "my_table", max_rows_per_output_parquet=2
    )

    assert outputs is not None
    assert outputs.left.total_row_count == 5
    assert outputs.left.rows_written == 2
    assert len(pl.read_parquet(outputs.left.path)) == 2
    assert outputs.right.total_row_count == 0
    assert outputs.right.rows_written == 0


def test_write_row_diff_parquet_bytes_and_hash(tmp_path: Path):
    left = pl.LazyFrame({"x": [1]})
    right = pl.LazyFrame({"x": []}, schema={"x": pl.Int64})
    row_diff = compare_rows_without_pk(left, right)

    outputs = write_row_diff_parquet(row_diff, tmp_path / "out", "my_table")

    assert outputs is not None
    file_bytes = outputs.left.path.read_bytes()
    assert outputs.left.bytes == len(file_bytes)
    assert outputs.left.hash == f"sha256:{hashlib.sha256(file_bytes).hexdigest()}"


def test_write_row_diff_parquet_none_row_diff_writes_nothing(tmp_path: Path):
    output_dir = tmp_path / "out"
    outputs = write_row_diff_parquet(None, output_dir, "my_table")
    assert outputs is None
    assert not output_dir.exists()
