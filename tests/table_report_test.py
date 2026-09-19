"""Unit tests for pudl.validate.diff.table_report."""

import json
from datetime import date
from pathlib import Path

import polars as pl

from pudl.validate.diff import table as diff_table
from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.formatting import format_bytes
from pudl.validate.diff.outputs import write_row_diff_parquet
from pudl.validate.diff.row_counts import (
    compare_row_counts,
)
from pudl.validate.diff.table import run_table_diff


def test_report_partition_expr_is_the_dbt_sql_expression(
    tmp_path: Path, mocker, no_pk_resource, make_dataset
):
    """The report should name the dbt SQL expression, not Polars' rendering of it."""
    resources = [no_pk_resource("table_without_pk")]
    left = make_dataset(
        tmp_path / "left",
        resources,
        {
            "table_without_pk": pl.DataFrame(
                {"ts": [date(2020, 1, 1), date(2021, 1, 1)], "y": [1, 2]}
            )
        },
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {"table_without_pk": pl.DataFrame({"ts": [date(2020, 1, 1)], "y": [1]})},
    )
    mocker.patch.object(
        diff_table, "get_dbt_partition_expr", return_value=" EXTRACT(YEAR FROM ts) "
    )
    run = run_table_diff(left, right, "table_without_pk")
    report = table_report.build_table_diff_report(run, left, right, "table_without_pk")

    assert report.row_count_diff is not None
    assert report.row_count_diff.partition_expr == "EXTRACT(YEAR FROM ts)"
    assert [c.partition for c in report.row_count_diff.changes] == [2021]


def test_row_count_summary_per_partition_fields():
    left = pl.LazyFrame({"year": [2020, 2020, 2020, 2021]})
    right = pl.LazyFrame({"year": [2020, 2021, 2021, 2022]})
    summary = table_report.RowCountDiffSummary.from_row_count_diff(
        compare_row_counts(left, right, partition_expr="year")
    )
    by_partition = {change.partition: change for change in summary.changes}
    assert by_partition[2020].model_dump() == {
        "partition": 2020,
        "left_row_count": 3,
        "right_row_count": 1,
        "row_count_difference": -2,
    }
    assert by_partition[2021].row_count_difference == 1
    # A partition missing from one side counts as having no rows there.
    assert by_partition[2022].model_dump() == {
        "partition": 2022,
        "left_row_count": None,
        "right_row_count": 1,
        "row_count_difference": 1,
    }


def test_row_count_summary_partition_values_use_native_json_types():
    left = pl.LazyFrame(
        {
            "state": ["CO", "NM"],
            "day": [date(2020, 1, 1), date(2020, 1, 2)],
            "flag": [True, False],
        }
    )
    right = left.head(0)
    summaries = {
        column: table_report.RowCountDiffSummary.from_row_count_diff(
            compare_row_counts(left, right, partition_expr=column)
        )
        for column in ["state", "day", "flag"]
    }
    assert {c.partition for c in summaries["state"].changes} == {"CO", "NM"}
    assert {c.partition for c in summaries["day"].changes} == {
        "2020-01-01",
        "2020-01-02",
    }
    flags = {c.partition for c in summaries["flag"].changes}
    assert flags == {False, True}
    assert all(isinstance(flag, bool) for flag in flags)
    parsed = json.loads(summaries["flag"].model_dump_json())
    assert {c["partition"] for c in parsed["changes"]} == {True, False}


def test_report_partition_expr_for_explicit_column_and_expression():
    left = pl.LazyFrame({"x": [1, 2]})
    right = pl.LazyFrame({"x": [1]})
    by_column = table_report.RowCountDiffSummary.from_row_count_diff(
        compare_row_counts(left, right, partition_expr="x")
    )
    assert by_column.partition_expr == "x"
    by_expr = table_report.RowCountDiffSummary.from_row_count_diff(
        compare_row_counts(left, right, partition_expr=pl.col("x") // 2)
    )
    assert by_expr.partition_expr == str(pl.col("x") // 2)


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
    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
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
    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
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
    run = run_table_diff(left, right, "table_without_pk", auto_partition=False)
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
    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
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
        auto_partition=False,
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
    run = run_table_diff(left, right, "table_with_pk", auto_partition=False)
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
