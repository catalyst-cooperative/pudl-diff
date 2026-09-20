"""Unit tests for pudl.validate.diff.dataset_report."""

import json
from pathlib import Path

import polars as pl
import pydantic
import pytest

from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import (
    PudlDiffReport,
    build_pudl_diff_report,
)
from pudl.validate.diff.runner import run_dataset_diff


def test_build_pudl_diff_report_dataset_provenance(tmp_path: Path, pk_resource):
    resource = pk_resource("table_with_pk", ["x"])
    left_root = tmp_path / "left"
    right_root = tmp_path / "right"
    left_root.mkdir()
    right_root.mkdir()
    (left_root / "datapackage.json").write_text(
        json.dumps(
            {
                "name": "left",
                "id": "abc-123",
                "created": "2026-01-01T00:00:00+00:00",
                "git_sha": "deadbeef",
                "git_tags": ["v2026.1.0"],
                "resources": [resource],
            }
        )
    )
    (right_root / "datapackage.json").write_text(
        json.dumps({"name": "right", "resources": [resource]})
    )
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_parquet(
        left_root / "table_with_pk.parquet"
    )
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_parquet(
        right_root / "table_with_pk.parquet"
    )
    left = PudlDiffDataset(left_root)
    right = PudlDiffDataset(right_root)

    table_diff_report = table_report.report_table_diff(
        left, right, "table_with_pk", tmp_path / "out"
    )
    report = build_pudl_diff_report(left, right, {"table_with_pk": table_diff_report})

    assert report.left_dataset.root == str(left_root)
    assert report.left_dataset.id == "abc-123"
    assert report.left_dataset.created == "2026-01-01T00:00:00+00:00"
    assert report.left_dataset.git_sha == "deadbeef"
    assert report.left_dataset.git_tags == ["v2026.1.0"]
    # the right dataset's datapackage.json has none of these fields
    assert report.right_dataset.root == str(right_root)
    assert report.right_dataset.id is None
    assert report.right_dataset.created is None
    assert report.right_dataset.git_sha is None
    assert report.right_dataset.git_tags is None


def test_pudl_diff_report_summary_and_status(tmp_path: Path, pk_resource, make_dataset):
    resources = [
        pk_resource("same", ["x"]),
        pk_resource("changed", ["x"]),
        pk_resource("broken", ["x"]),
    ]
    frame = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    left = make_dataset(
        tmp_path / "left",
        resources,
        {"same": frame, "changed": frame, "broken": frame},
    )
    right = make_dataset(
        tmp_path / "right",
        resources,
        {
            "same": frame,
            "changed": pl.DataFrame({"x": [1, 2, 3], "y": ["a", "z", "c"]}),
            "broken": frame,
        },
    )
    (tmp_path / "right" / "broken.parquet").write_bytes(b"not a parquet file")

    tables = {
        name: table_report.report_table_diff(
            left,
            right,
            name,
            tmp_path / "out",
            options=table_report.DiffOptions(auto_partition=False),
        )
        for name in ["broken", "changed", "same"]
    }
    report = build_pudl_diff_report(
        left,
        right,
        tables,
        tables_only_in_left=["z", "a"],
        elapsed_seconds=1.5,
    )

    assert report.schema_version == "1.0.0"
    assert list(report.tables) == ["broken", "changed", "same"]
    assert report.tables_only_in_left == ["a", "z"]
    assert not report.success
    assert not report.is_identical
    assert report.error is None
    assert report.exit_code == 2
    summary = report.summary
    assert summary.table_count == 3
    assert summary.identical_table_count == 1
    assert summary.changed_table_count == 1
    assert summary.failed_table_count == 1
    assert summary.failed_tables == ["broken"]
    assert summary.rows_added == 1
    assert summary.rows_changed == 1
    assert summary.rows_removed == 0
    assert summary.no_row_diff_table_count == 1
    assert summary.left_table_bytes is not None
    assert summary.left_table_bytes == sum(
        t.left_table_bytes or 0 for t in tables.values()
    )
    assert summary.peak_rss_table in {"changed", "same"}
    json.loads(report.model_dump_json())  # serializes cleanly


def test_pudl_diff_report_exit_codes(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("t", ["x"])]
    frame = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    left = make_dataset(tmp_path / "left", resources, {"t": frame})
    right = make_dataset(tmp_path / "right", resources, {"t": frame})
    table = table_report.report_table_diff(left, right, "t", tmp_path / "out")

    identical = build_pudl_diff_report(left, right, {"t": table})
    assert identical.is_identical
    assert identical.success
    assert identical.exit_code == 0

    # A run-level error fails the whole report even if every table succeeded.
    failed = build_pudl_diff_report(left, right, {"t": table}, error="boom")
    assert not failed.success
    assert not failed.is_identical
    assert failed.error == "boom"
    assert failed.exit_code == 2


def test_pudl_diff_report_round_trips_through_json(tmp_path: Path, write_two_datasets):
    left, right = write_two_datasets(
        tmp_path,
        {"table_a": ["a", "b"], "table_b": ["a", "b"], "table_left": ["a"]},
        {"table_a": ["a", "b"], "table_b": ["a", "c"], "table_right": ["a"]},
    )
    report = run_dataset_diff(left, right, tmp_path / "out")

    loaded = PudlDiffReport.model_validate_json(report.model_dump_json())

    assert loaded == report
    assert loaded.tables["table_b"].row_diff is not None
    assert loaded.exit_code == report.exit_code == 1


def test_pudl_diff_report_with_an_error_round_trips_through_json(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path, {"table_left": ["a"]}, {"table_right": ["a"]}
    )
    report = run_dataset_diff(left, right, tmp_path / "out")

    loaded = PudlDiffReport.model_validate_json(report.model_dump_json())

    assert loaded == report
    assert loaded.error == report.error
    assert loaded.exit_code == 2


def test_derived_fields_are_recomputed_when_a_report_is_loaded(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path, {"table_a": ["a", "b"]}, {"table_a": ["a", "c"]}
    )
    report = run_dataset_diff(left, right, tmp_path / "out")
    assert not report.is_identical
    document = json.loads(report.model_dump_json())
    # The derived fields are in the JSON...
    assert document["is_identical"] is False
    assert document["success"] is True
    assert document["tables"]["table_a"]["is_identical"] is False

    # ...but a loaded report doesn't take them on trust.
    document["is_identical"] = True
    document["tables"]["table_a"]["is_identical"] = True
    loaded = PudlDiffReport.model_validate(document)

    assert not loaded.is_identical
    assert not loaded.tables["table_a"].is_identical
    assert loaded == report


def test_row_diff_sections_load_as_the_variant_their_status_names(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path, {"table_a": ["a", "b"]}, {"table_a": ["a", "c"]}
    )
    report = run_dataset_diff(left, right, tmp_path / "out")
    document = json.loads(report.model_dump_json())
    row_diff = document["tables"]["table_a"]["row_diff"]
    assert row_diff["pk_diff"]["status"] == "compared"
    assert row_diff["non_pk_diff"] == {
        "status": "skipped",
        "skipped_reason": "primary_key_available",
    }

    loaded = PudlDiffReport.model_validate(document)
    assert loaded == report
    loaded_row_diff = loaded.tables["table_a"].row_diff
    assert loaded_row_diff is not None
    assert isinstance(loaded_row_diff.pk_diff, table_report.PkRowDiffSummary)
    assert isinstance(loaded_row_diff.non_pk_diff, table_report.RowDiffSectionSkipped)

    # The status is what decides, so one that isn't a variant of the section is an error.
    row_diff["pk_diff"]["status"] = "unknown"
    with pytest.raises(pydantic.ValidationError, match="does not match any of the"):
        PudlDiffReport.model_validate(document)
