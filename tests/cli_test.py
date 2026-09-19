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
    assert report["row_diff"]["pk_diff"]["changed_row_count"] == 1
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
    assert report["row_diff"]["pk_diff"] == {"skipped_reason": "too_many_rows"}
    assert report["row_diff"]["non_pk_diff"] == {
        "skipped_reason": "primary_key_available"
    }
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


def _all_tables_datasets(tmp_path: Path) -> tuple[Path, Path]:
    """Two datasets sharing an identical and a changed table, plus one table
    unique to each side."""
    resources = [
        _pk_resource("same_table", ["x"]),
        _pk_resource("changed_table", ["x"]),
        _no_pk_resource("left_only_table"),
        _no_pk_resource("right_only_table"),
    ]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    left = tmp_path / "left"
    right = tmp_path / "right"
    _make_dataset(
        left,
        resources,
        {"same_table": df, "changed_table": df, "left_only_table": df},
    )
    _make_dataset(
        right,
        resources,
        {
            "same_table": df,
            "changed_table": df.with_columns(y=pl.lit("z")),
            "right_only_table": df,
        },
    )
    return left, right


def test_no_table_name_compares_every_table_in_both_datasets(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "--left",
            str(left),
            "--right",
            str(right),
            "--output-path",
            str(output_path),
        ],
    )

    # One table differs, so the overall exit code is 1.
    assert result.exit_code == 1, result.output
    assert sorted(p.name for p in output_path.glob("*_diff.json")) == [
        "changed_table_diff.json",
        "same_table_diff.json",
    ]
    same = json.loads((output_path / "same_table_diff.json").read_text())
    changed = json.loads((output_path / "changed_table_diff.json").read_text())
    assert same["is_identical"] is True
    assert changed["is_identical"] is False
    assert (output_path / "changed_table_left_only.parquet").exists()
    assert "Identical: 1  Different: 1  Failed: 0" in result.output
    assert "Different: changed_table" in result.output
    assert "Only in left: left_only_table" in result.output
    assert "Only in right: right_only_table" in result.output


def test_no_table_name_all_identical_exits_zero(tmp_path: Path):
    resources = [_pk_resource("a", ["x"]), _pk_resource("b", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    _make_dataset(tmp_path / "left", resources, {"a": df, "b": df})
    _make_dataset(tmp_path / "right", resources, {"a": df, "b": df})

    result = CliRunner().invoke(
        main,
        [
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Identical: 2  Different: 0  Failed: 0" in result.output


def test_no_table_name_failed_comparison_exits_two(tmp_path: Path):
    """A table that fails to compare is reported, and doesn't stop the others."""
    resources = [_pk_resource("a", ["x"]), _pk_resource("b", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    _make_dataset(tmp_path / "left", resources, {"a": df, "b": df})
    _make_dataset(tmp_path / "right", resources, {"a": df, "b": df})
    # Corrupt one table on the right so that comparing it raises.
    (tmp_path / "right" / "a.parquet").write_bytes(b"not a parquet file")

    result = CliRunner().invoke(
        main,
        [
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Identical: 1  Different: 0  Failed: 1" in result.output
    assert "Failed: a" in result.output
    assert (tmp_path / "out" / "b_diff.json").exists()
    report = json.loads((tmp_path / "out" / "a_diff.json").read_text())
    assert report["success"] is False


def test_no_table_name_no_tables_in_common_exits_two(tmp_path: Path):
    df = pl.DataFrame({"x": [1]})
    _make_dataset(tmp_path / "left", [_no_pk_resource("a")], {"a": df})
    _make_dataset(tmp_path / "right", [_no_pk_resource("b")], {"b": df})

    result = CliRunner().invoke(
        main,
        ["--left", str(tmp_path / "left"), "--right", str(tmp_path / "right")],
    )

    assert result.exit_code == 2
    assert "No tables found in both" in result.output


def test_right_table_and_partition_expr_require_a_table_name(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)
    base = ["--left", str(left), "--right", str(right)]

    # --right-table needs exactly one table: none, or more than one, is an error.
    result = CliRunner().invoke(main, [*base, "--right-table", "x"])
    assert result.exit_code == 2
    assert "--right-table requires exactly one TABLE_NAME" in result.output

    result = CliRunner().invoke(
        main, [*base, "--right-table", "x", "same_table", "changed_table"]
    )
    assert result.exit_code == 2
    assert "--right-table requires exactly one TABLE_NAME" in result.output

    result = CliRunner().invoke(main, [*base, "--partition-expr", "x"])
    assert result.exit_code == 2
    assert "--partition-expr requires at least one TABLE_NAME" in result.output


def test_multiple_table_names_compares_only_those_tables(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "changed_table",
            "same_table",
            "changed_table",  # duplicates are only compared once
            "--left",
            str(left),
            "--right",
            str(right),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 1, result.output
    assert sorted(p.name for p in output_path.glob("*_diff.json")) == [
        "changed_table_diff.json",
        "same_table_diff.json",
    ]
    assert "Comparing 2 tables between" in result.output
    assert "Identical: 1  Different: 1  Failed: 0" in result.output
    # Tables that weren't asked for aren't listed as one-sided.
    assert "Only in" not in result.output


def test_multiple_table_names_including_a_missing_table_exits_two(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "same_table",
            "left_only_table",  # missing from the right dataset
            "--left",
            str(left),
            "--right",
            str(right),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Identical: 1  Different: 0  Failed: 1" in result.output
    assert "Failed: left_only_table" in result.output
    assert (output_path / "same_table_diff.json").exists()
