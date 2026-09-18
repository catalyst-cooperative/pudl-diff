"""Unit tests for the pudl_diff script."""

import json
from pathlib import Path

import polars as pl
from click.testing import CliRunner

from pudl.scripts.pudl_diff import main


def _write_datapackage(root: Path, resources: list[dict]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "datapackage.json").write_text(
        json.dumps({"name": "test", "resources": resources})
    )


def _pk_resource(name: str, primary_key: list[str]) -> dict:
    return {
        "name": name,
        "schema": {
            "fields": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "string"},
            ],
            "primaryKey": primary_key,
        },
    }


def _no_pk_resource(name: str) -> dict:
    return {
        "name": name,
        "schema": {
            "fields": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "string"},
            ],
        },
    }


def _make_dataset(
    root: Path, resources: list[dict], tables: dict[str, pl.DataFrame]
) -> None:
    _write_datapackage(root, resources)
    for table_name, df in tables.items():
        df.write_parquet(root / f"{table_name}.parquet")


def test_identical_table_exits_zero(tmp_path: Path):
    resources = [_pk_resource("table_with_pk", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [2, 1], "y": ["b", "a"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_with_pk",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads((output_path / "table_with_pk_diff.json").read_text())
    assert report["success"] is True
    assert report["is_identical"] is True


def test_differing_pk_table_exits_one_and_writes_parquet(tmp_path: Path):
    resources = [_pk_resource("table_with_pk", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "changed"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_with_pk",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 1, result.output
    report = json.loads((output_path / "table_with_pk_diff.json").read_text())
    assert report["success"] is True
    assert report["is_identical"] is False
    assert report["row_diff"]["pk_diff"]["mismatched_row_count"] == 1
    assert (output_path / "table_with_pk_left_only.parquet").exists()
    assert (output_path / "table_with_pk_right_only.parquet").exists()


def test_differing_non_pk_table_exits_one(tmp_path: Path):
    resources = [_no_pk_resource("table_without_pk")]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 3], "y": ["a", "c"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_without_pk",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 1, result.output
    report = json.loads((output_path / "table_without_pk_diff.json").read_text())
    assert report["row_diff"]["non_pk_diff"]["symmetric_difference_count"] == 2


def test_unknown_table_exits_two(tmp_path: Path):
    resources = [_pk_resource("table_with_pk", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "nonexistent_table",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 2, result.output
    report = json.loads((output_path / "nonexistent_table_diff.json").read_text())
    assert report["success"] is False
    assert "FileNotFoundError" in report["error"]


def test_missing_dataset_exits_two(tmp_path: Path):
    """A dataset root with no datapackage.json at all is also an exit-2 error."""
    left_root = tmp_path / "left"
    left_root.mkdir()
    _make_dataset(
        tmp_path / "right",
        [_pk_resource("table_with_pk", ["x"])],
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_with_pk",
            "--left",
            str(left_root),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 2, result.output
    report = json.loads((output_path / "table_with_pk_diff.json").read_text())
    assert report["success"] is False


def test_large_table_skip_still_exits_one(tmp_path: Path):
    resources = [_pk_resource("table_with_pk", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_with_pk",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
            "--max-compare-rows",
            "1",
        ],
    )

    # is_identical is conservatively False since row-level comparison never ran.
    assert result.exit_code == 1, result.output
    report = json.loads((output_path / "table_with_pk_diff.json").read_text())
    assert report["success"] is True
    assert report["is_identical"] is False
    assert report["row_diff"]["skipped_reason"] == "too_many_rows"
    assert not (output_path / "table_with_pk_left_only.parquet").exists()


def test_max_rows_per_output_parquet(tmp_path: Path):
    resources = [_no_pk_resource("table_without_pk")]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2, 3, 4, 5], "y": list("abcde")})},
    )
    _make_dataset(
        tmp_path / "right",
        resources,
        {
            "table_without_pk": pl.DataFrame(
                {"x": [], "y": []}, schema={"x": pl.Int64, "y": pl.String}
            )
        },
    )
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "table_without_pk",
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
            "--max-output-rows",
            "2",
        ],
    )

    assert result.exit_code == 1, result.output
    left_only = pl.read_parquet(output_path / "table_without_pk_left_only.parquet")
    assert len(left_only) == 2
    report = json.loads((output_path / "table_without_pk_diff.json").read_text())
    assert report["row_diff"]["non_pk_diff"]["only_in_left_count"] == 5
