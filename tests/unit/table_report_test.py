"""Unit tests for pudl_diff.table_report."""

import contextlib
import json
from pathlib import Path

import polars as pl
import pytest

from pudl_diff import table_report
from pudl_diff.dataset import PudlDiffDataset
from pudl_diff.formatting import format_bytes
from pudl_diff.outputs import write_row_diff_parquet
from pudl_diff.row_counts import (
    compare_row_counts,
)
from pudl_diff.table import MAX_ROWS_FOR_ROW_LEVEL_COMPARISON, run_table_diff


def test_row_count_summary_fields():
    left = pl.LazyFrame({"year": [2020, 2020, 2020, 2021]})
    right = pl.LazyFrame({"year": [2020, 2021, 2021]})
    summary = table_report.RowCountDiffSummary.from_row_count_diff(
        compare_row_counts(left, right)
    )
    assert summary.left_row_count == 4
    assert summary.right_row_count == 3
    assert summary.row_count_difference == -1
    assert not summary.is_identical


def test_build_table_diff_report_identical_pk_table(
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
        {"table_with_pk": pl.DataFrame({"x": [2, 1], "y": ["b", "a"]})},
    )
    run = run_table_diff(left, right, "table_with_pk")
    report = table_report.build_table_diff_report(run, left, right, "table_with_pk")

    assert report.success
    assert report.error is None
    assert report.is_identical
    assert report.peak_rss_bytes is not None
    assert report.peak_rss == format_bytes(report.peak_rss_bytes)
    left_bytes = (tmp_path / "left" / "table_with_pk.parquet").stat().st_size
    right_bytes = (tmp_path / "right" / "table_with_pk.parquet").stat().st_size
    assert report.left_table_bytes == left_bytes
    assert report.right_table_bytes == right_bytes
    assert report.bytes_difference == right_bytes - left_bytes
    assert report.left_table_name == "table_with_pk"
    assert report.right_table_name == "table_with_pk"
    assert report.left_table_path.endswith("table_with_pk.parquet")
    assert report.right_table_path.endswith("table_with_pk.parquet")
    assert report.schema_diff is not None
    assert report.schema_diff.is_identical
    assert report.row_count_diff is not None
    assert report.row_count_diff.is_identical
    assert report.row_count_diff.left_row_count == 2
    assert report.row_count_diff.right_row_count == 2
    assert report.row_diff is not None
    assert isinstance(report.row_diff.pk_diff, table_report.PkRowDiffSummary)
    assert report.row_diff.pk_diff.is_identical
    assert report.row_diff.non_pk_diff == table_report.RowDiffSectionSkipped(
        skipped_reason="primary_key_available"
    )
    assert report.row_diff.left_only_parquet is None
    assert report.row_diff.right_only_parquet is None
    # round-trips through JSON cleanly
    parsed = json.loads(report.model_dump_json())
    assert parsed["success"] is True


def test_build_table_diff_report_differing_pk_table(
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
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "changed"]})},
    )
    run = run_table_diff(left, right, "table_with_pk")
    assert run.result is not None
    outputs = write_row_diff_parquet(
        run.result.row_diff, tmp_path / "out", "table_with_pk"
    )
    report = table_report.build_table_diff_report(
        run, left, right, "table_with_pk", parquet_outputs=outputs
    )

    assert report.success
    assert not report.is_identical
    assert report.row_diff is not None
    assert isinstance(report.row_diff.pk_diff, table_report.PkRowDiffSummary)
    assert report.row_diff.pk_diff.primary_keys_identical
    assert report.row_diff.pk_diff.changed_row_count == 1
    assert report.row_diff.pk_diff.column_changes == {"y": 1}
    assert report.row_diff.left_only_parquet is not None
    assert report.row_diff.right_only_parquet is not None
    assert report.row_diff.left_only_parquet.bytes > 0
    assert report.row_diff.left_only_parquet.hash.startswith("sha256:")


def test_build_table_diff_report_differing_non_pk_table(
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
    run = run_table_diff(left, right, "table_without_pk")
    report = table_report.build_table_diff_report(run, left, right, "table_without_pk")

    assert report.success
    assert not report.is_identical
    assert report.row_diff is not None
    assert report.row_diff.pk_diff == table_report.RowDiffSectionSkipped(
        skipped_reason="no_primary_key"
    )
    assert isinstance(report.row_diff.non_pk_diff, table_report.NonPkRowDiffSummary)
    assert report.row_diff.non_pk_diff.only_in_left_count == 1
    assert report.row_diff.non_pk_diff.only_in_right_count == 1
    assert report.row_diff.non_pk_diff.symmetric_difference_count == 2
    assert report.row_diff.non_pk_diff.multiplicity_changed_row_count == 0
    assert not report.row_diff.non_pk_diff.is_identical


def test_row_count_difference_is_right_minus_left():
    left = pl.LazyFrame({"x": [1, 2]})
    right = pl.LazyFrame({"x": [1, 2, 3, 4, 5]})
    summary = table_report.RowCountDiffSummary.from_row_count_diff(
        compare_row_counts(left, right)
    )
    assert summary.row_count_difference == 3


def test_build_table_diff_report_schema_mismatch(
    tmp_path: Path, pk_resource, make_dataset
):
    left_resources = [pk_resource("table_with_pk", ["x"])]
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
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    right = make_dataset(
        tmp_path / "right",
        right_resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "z": [1, 2]})},
    )
    run = run_table_diff(left, right, "table_with_pk")
    report = table_report.build_table_diff_report(run, left, right, "table_with_pk")

    assert report.success
    assert not report.is_identical
    assert report.schema_diff is not None
    assert not report.schema_diff.is_identical
    assert report.schema_diff.columns_only_in_right == ["z"]


def test_build_table_diff_report_skipped_large_table(
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
    run = run_table_diff(
        left,
        right,
        "table_with_pk",
        max_rows_for_row_level_comparison=1,
    )
    report = table_report.build_table_diff_report(run, left, right, "table_with_pk")

    assert report.success
    assert not report.is_identical
    assert report.row_diff is not None
    assert report.row_diff.pk_diff == table_report.RowDiffSectionSkipped(
        skipped_reason="too_many_rows"
    )
    assert report.row_diff.non_pk_diff == table_report.RowDiffSectionSkipped(
        skipped_reason="primary_key_available"
    )


def test_build_table_diff_report_error_case(tmp_path: Path, pk_resource, make_dataset):
    left_root = tmp_path / "left"
    left_root.mkdir()
    left = PudlDiffDataset(left_root)
    right = make_dataset(
        tmp_path / "right",
        [pk_resource("table_with_pk", ["x"])],
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    run = run_table_diff(left, right, "table_with_pk")
    report = table_report.build_table_diff_report(run, left, right, "table_with_pk")

    assert not report.success
    assert not report.is_identical
    assert report.error is not None
    assert "FileNotFoundError" in report.error
    # table paths are still reportable even though left's datapackage.json
    # itself couldn't be read - the path is deterministic from the root.
    assert report.left_table_path.endswith("table_with_pk.parquet")
    assert report.right_table_path.endswith("table_with_pk.parquet")
    assert report.schema_diff is None
    assert report.row_count_diff is None
    assert report.row_diff is None
    assert report.elapsed_seconds is None
    # sizes are best-effort: known for the side that exists, unknown for the rest
    assert report.left_table_bytes is None
    assert report.right_table_bytes is not None
    assert report.bytes_difference is None
    assert report.bytes_difference_percent is None
    # still JSON-serializable despite the failure
    json.loads(report.model_dump_json())


def test_size_comparison_derived_fields():
    sizes = table_report.SizeComparison(
        left_table_bytes=2_000_000, right_table_bytes=1_500_000
    )
    parsed = json.loads(sizes.model_dump_json())
    assert parsed == {
        "left_table_bytes": 2_000_000,
        "right_table_bytes": 1_500_000,
        "left_table_size": "2.0 MB",
        "right_table_size": "1.5 MB",
        "bytes_difference": -500_000,
        "bytes_difference_size": "-500.0 KB",
        "bytes_difference_percent": -25.0,
    }


def test_size_comparison_with_unknown_or_empty_left_size():
    unknown = table_report.SizeComparison(left_table_bytes=None, right_table_bytes=10)
    assert unknown.bytes_difference is None
    assert unknown.bytes_difference_size is None
    assert unknown.bytes_difference_percent is None
    assert unknown.right_table_size == "10 B"

    empty_left = table_report.SizeComparison(left_table_bytes=0, right_table_bytes=10)
    assert empty_left.bytes_difference == 10
    assert empty_left.bytes_difference_percent is None


@pytest.mark.parametrize("has_pk", [True, False], ids=["pk", "no_pk"])
@pytest.mark.parametrize(
    ("right", "max_rows"),
    [
        (pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}), None),  # identical
        (pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "z"]}), None),  # a value changed
        (pl.DataFrame({"x": [1, 2, 3, 4], "y": list("abcd")}), None),  # a row added
        (pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}), None),  # a row removed
        (pl.DataFrame({"x": [1, 2, 3], "z": list("abc")}), None),  # columns changed
        (pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}), 1),  # rows not compared
    ],
    ids=["identical", "changed", "added", "removed", "columns", "not_compared"],
)
def test_table_report_derived_fields_agree_with_the_comparison(
    has_pk: bool,
    right: pl.DataFrame,
    max_rows: int | None,
    tmp_path: Path,
    pk_resource,
    no_pk_resource,
    make_dataset,
):
    """`is_identical` and `success` are derived from the report's other fields."""
    resource = pk_resource("t", ["x"]) if has_pk else no_pk_resource("t")
    left_df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    left = make_dataset(tmp_path / "left", [resource], {"t": left_df})
    right_ds = make_dataset(tmp_path / "right", [resource], {"t": right})
    run = run_table_diff(
        left,
        right_ds,
        "t",
        max_rows_for_row_level_comparison=max_rows or MAX_ROWS_FOR_ROW_LEVEL_COMPARISON,
    )
    report = table_report.build_table_diff_report(run, left, right_ds, "t")

    assert run.result is not None
    assert report.success
    assert report.is_identical == run.result.is_identical
    assert report.schema_diff is not None
    assert report.schema_diff.is_identical == run.result.schema_diff.is_identical
    assert report.row_count_diff is not None
    assert report.row_count_diff.is_identical == run.result.row_count_diff.is_identical
    if run.result.row_diff is not None:
        assert report.row_diff is not None
        summary = report.row_diff.pk_diff if has_pk else report.row_diff.non_pk_diff
        assert isinstance(
            summary,
            table_report.PkRowDiffSummary | table_report.NonPkRowDiffSummary,
        )
        assert summary.is_identical == run.result.row_diff.is_identical
    else:
        # Without a row-level comparison, the report can't call the table identical.
        assert not report.is_identical


def test_table_report_of_a_failed_comparison_is_not_a_success(
    tmp_path: Path, pk_resource, make_dataset
):
    left = make_dataset(
        tmp_path / "left", [pk_resource("t", ["x"])], {"t": pl.DataFrame({"x": [1]})}
    )
    right = PudlDiffDataset(tmp_path / "nowhere")
    run = run_table_diff(left, right, "t")

    report = table_report.build_table_diff_report(run, left, right, "t")

    assert report.error is not None
    assert not report.success
    assert not report.is_identical


def test_row_count_summary_is_identical_only_if_the_totals_match():
    def summary(left: int, right: int) -> table_report.RowCountDiffSummary:
        return table_report.RowCountDiffSummary(
            left_row_count=left,
            right_row_count=right,
            row_count_difference=right - left,
        )

    assert summary(5, 5).is_identical
    assert not summary(5, 6).is_identical


def test_parquet_output_paths_are_relative_to_the_report_directory(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("t", ["x"])]
    make_dataset(tmp_path / "left", resources, {"t": pl.DataFrame({"x": [1, 2]})})
    make_dataset(tmp_path / "right", resources, {"t": pl.DataFrame({"x": [2, 3]})})

    with contextlib.chdir(tmp_path):
        # The datasets and the output directory are all given relative to the
        # working directory, which is not where the report will be.
        report = table_report.report_table_diff(
            PudlDiffDataset("left"), PudlDiffDataset("right"), "t", "some/out"
        )

    assert report.row_diff is not None
    assert report.row_diff.left_only_parquet is not None
    assert report.row_diff.right_only_parquet is not None
    assert report.row_diff.left_only_parquet.path == "t_left_only.parquet"
    assert report.row_diff.right_only_parquet.path == "t_right_only.parquet"
    assert (tmp_path / "some/out" / "t_left_only.parquet").exists()
    # The dataset paths, though, are absolute.
    assert Path(report.left_table_path) == (tmp_path / "left" / "t.parquet").resolve()
    assert Path(report.right_table_path) == (tmp_path / "right" / "t.parquet").resolve()


def test_parquet_output_paths_are_relative_to_a_report_directory_above_them(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("t", ["x"])]
    left = make_dataset(
        tmp_path / "left", resources, {"t": pl.DataFrame({"x": [1, 2]})}
    )
    right = make_dataset(
        tmp_path / "right", resources, {"t": pl.DataFrame({"x": [2, 3]})}
    )
    run = run_table_diff(left, right, "t")
    assert run.result is not None
    outputs = write_row_diff_parquet(
        run.result.row_diff, tmp_path / "out" / "files", "t"
    )

    report = table_report.build_table_diff_report(
        run, left, right, "t", parquet_outputs=outputs, report_dir=tmp_path / "out"
    )

    assert report.row_diff is not None
    assert report.row_diff.left_only_parquet is not None
    assert report.row_diff.left_only_parquet.path == "files/t_left_only.parquet"
    # Without being told, they're taken to be in the report's directory.
    report = table_report.build_table_diff_report(
        run, left, right, "t", parquet_outputs=outputs
    )
    assert report.row_diff is not None
    assert report.row_diff.left_only_parquet is not None
    assert report.row_diff.left_only_parquet.path == "t_left_only.parquet"


def test_report_table_diff_removes_the_temporary_files_of_the_differing_rows(
    tmp_path: Path, pk_resource, make_dataset, mocker
):
    resources = [pk_resource("t", ["x"])]
    left = make_dataset(
        tmp_path / "left", resources, {"t": pl.DataFrame({"x": [1, 2]})}
    )
    right = make_dataset(
        tmp_path / "right", resources, {"t": pl.DataFrame({"x": [2, 3]})}
    )
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    mocker.patch("tempfile.tempdir", str(temp_dir))

    report = table_report.report_table_diff(left, right, "t", tmp_path / "out")

    assert report.row_diff is not None
    assert report.row_diff.left_only_parquet is not None  # the rows were written
    assert list(temp_dir.iterdir()) == []


def test_a_row_diff_summary_needs_a_reason_if_the_rows_were_not_compared(
    tmp_path: Path,
):
    with pytest.raises(AssertionError, match="no skip reason"):
        table_report._build_row_diff_summary(None, [], None, None, tmp_path)
