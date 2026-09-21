"""Unit tests for pudl_diff.runner."""

from pathlib import Path

from pudl_diff.runner import run_dataset_diff


def test_run_dataset_diff_reports_progress_and_builds_the_report(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path,
        {"table_a": ["a", "b"], "table_b": ["a", "b"], "table_left": ["a"]},
        {"table_a": ["a", "b"], "table_b": ["a", "c"], "table_right": ["a"]},
    )
    resolved: list[list[str]] = []
    compared: list[str] = []

    report = run_dataset_diff(
        left,
        right,
        tmp_path / "out",
        on_tables_resolved=resolved.append,
        on_table_compared=lambda name, table_report: compared.append(name),
    )

    assert resolved == [["table_a", "table_b"]]
    assert compared == ["table_a", "table_b"]
    assert list(report.tables) == ["table_a", "table_b"]
    assert report.tables["table_a"].is_identical
    assert not report.tables["table_b"].is_identical
    assert report.tables_only_in_left == ["table_left"]
    assert report.tables_only_in_right == ["table_right"]
    assert report.elapsed_seconds is not None
    assert report.error is None
    assert report.exit_code == 1


def test_run_dataset_diff_compares_only_the_tables_given(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path,
        {"table_a": ["a"], "table_b": ["a"]},
        {"table_a": ["a"], "table_b": ["a"]},
    )

    report = run_dataset_diff(left, right, tmp_path / "out", table_names=["table_b"])

    assert list(report.tables) == ["table_b"]
    assert report.tables_only_in_left == []
    assert report.exit_code == 0


def test_run_dataset_diff_records_why_there_is_nothing_to_compare(
    tmp_path: Path, write_two_datasets
):
    left, right = write_two_datasets(
        tmp_path, {"table_left": ["a"]}, {"table_right": ["a"]}
    )
    resolved: list[list[str]] = []

    report = run_dataset_diff(
        left, right, tmp_path / "out", on_tables_resolved=resolved.append
    )

    assert resolved == []
    assert report.tables == {}
    assert report.error is not None
    assert "No tables found in both" in report.error
    assert report.exit_code == 2


def test_run_dataset_diff_reports_a_failure_to_list_the_tables(
    mock_loggers, tmp_path: Path, write_two_datasets, mocker
):
    left, right = write_two_datasets(tmp_path, {"t": ["a"]}, {"t": ["a"]})
    mocker.patch("pudl_diff.runner.resolve_tables", side_effect=OSError("no bucket"))

    report = run_dataset_diff(left, right, tmp_path / "out")

    assert report.error is not None
    assert "no bucket" in report.error
    assert not report.success
    assert report.tables == {}
    mock_loggers["runner"].exception.assert_called_once()
