"""Unit tests for the pudl_diff script."""

import contextlib
import json
import logging
import re
import shutil
from pathlib import Path

import polars as pl
import pytest
from click.testing import CliRunner

import pudl_diff
from pudl_diff.cli import (
    REPORT_FILENAME,
    _set_log_level,
    main,
)
from pudl_diff.logs import get_logger


def test_version_option_shows_the_package_version():
    result = CliRunner().invoke(main, ["--version"])

    assert result.exit_code == 0
    assert result.output == f"pudl_diff, version {pudl_diff.__version__}\n"


def test_identical_table_exits_zero(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
    assert report["success"] is True
    assert report["is_identical"] is True


def test_differing_pk_table_exits_one_and_writes_parquet(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("table_with_pk", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
    assert report["success"] is True
    assert report["is_identical"] is False
    assert report["row_diff"]["pk_diff"]["changed_row_count"] == 1
    assert (output_path / "table_with_pk_left_only.parquet").exists()
    assert (output_path / "table_with_pk_right_only.parquet").exists()


def test_differing_non_pk_table_exits_one(tmp_path: Path, no_pk_resource, make_dataset):
    resources = [no_pk_resource("table_without_pk")]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["table_without_pk"]
    assert report["row_diff"]["non_pk_diff"]["symmetric_difference_count"] == 2


def test_unknown_table_exits_two(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["nonexistent_table"]
    assert report["success"] is False
    assert "FileNotFoundError" in report["error"]


def test_missing_dataset_exits_two(tmp_path: Path, pk_resource, make_dataset):
    """A dataset root with no datapackage.json at all is also an exit-2 error."""
    left_root = tmp_path / "left"
    left_root.mkdir()
    make_dataset(
        tmp_path / "right",
        [pk_resource("table_with_pk", ["x"])],
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
    assert report["success"] is False


def test_large_table_skip_still_exits_one(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("table_with_pk", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_with_pk": pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
    assert report["success"] is True
    assert report["is_identical"] is False
    assert report["row_diff"]["pk_diff"] == {
        "status": "skipped",
        "skipped_reason": "too_many_rows",
    }
    assert report["row_diff"]["non_pk_diff"] == {
        "status": "skipped",
        "skipped_reason": "primary_key_available",
    }
    assert not (output_path / "table_with_pk_left_only.parquet").exists()


def test_max_rows_per_output_parquet(tmp_path: Path, no_pk_resource, make_dataset):
    resources = [no_pk_resource("table_without_pk")]
    make_dataset(
        tmp_path / "left",
        resources,
        {"table_without_pk": pl.DataFrame({"x": [1, 2, 3, 4, 5], "y": list("abcde")})},
    )
    make_dataset(
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
    report = _load_report(output_path)["tables"]["table_without_pk"]
    assert report["row_diff"]["non_pk_diff"]["only_in_left_count"] == 5


@pytest.fixture
def all_tables_datasets(
    tmp_path: Path, pk_resource, no_pk_resource, make_dataset
) -> tuple[Path, Path]:
    """Two datasets sharing an identical and a changed table, plus one table
    unique to each side."""
    resources = [
        pk_resource("same_table", ["x"]),
        pk_resource("changed_table", ["x"]),
        no_pk_resource("left_only_table"),
        no_pk_resource("right_only_table"),
    ]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    left = tmp_path / "left"
    right = tmp_path / "right"
    make_dataset(
        left,
        resources,
        {"same_table": df, "changed_table": df, "left_only_table": df},
    )
    make_dataset(
        right,
        resources,
        {
            "same_table": df,
            "changed_table": df.with_columns(y=pl.lit("z")),
            "right_only_table": df,
        },
    )
    return left, right


def test_no_table_name_compares_every_table_in_both_datasets(
    tmp_path: Path, all_tables_datasets
):
    left, right = all_tables_datasets
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
    # Everything goes in a single report, however many tables are compared.
    assert [p.name for p in output_path.glob("*.json")] == [REPORT_FILENAME]
    report = _load_report(output_path)
    assert report["schema_version"] == "1.0.0"
    assert report["pudl_diff_version"] == pudl_diff.__version__
    assert report["is_identical"] is False
    assert report["success"] is True
    assert report["error"] is None
    assert list(report["tables"]) == ["changed_table", "same_table"]
    assert report["tables"]["same_table"]["is_identical"] is True
    assert report["tables"]["changed_table"]["is_identical"] is False
    assert report["tables_only_in_left"] == ["left_only_table"]
    assert report["tables_only_in_right"] == ["right_only_table"]
    assert report["left_dataset"]["root"] == str(left)
    assert report["right_dataset"]["root"] == str(right)
    assert report["options"]["rtol"] == 1e-5
    summary = report["summary"]
    assert summary["table_count"] == 2
    assert summary["identical_table_count"] == 1
    assert summary["changed_table_count"] == 1
    assert summary["failed_table_count"] == 0
    assert summary["left_table_bytes"] == sum(
        t["left_table_bytes"] for t in report["tables"].values()
    )
    for table in report["tables"].values():
        assert table["left_table_bytes"] > 0
        assert table["bytes_difference"] == (
            table["right_table_bytes"] - table["left_table_bytes"]
        )
        assert table["left_table_size"].endswith(" B")
        assert "created" not in table
    assert (output_path / "changed_table_left_only.parquet").exists()
    assert "Identical: 1  Changed: 1  Error: 0" in result.output
    # Changed tables are counted, but not listed by name in the summary.
    assert "Changed: changed_table" not in result.output
    assert "Tables removed (only in left): 1\n  left_only_table\n" in result.output
    assert "Tables added (only in right): 1\n  right_only_table\n" in result.output


def test_no_table_name_all_identical_exits_zero(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("a", ["x"]), pk_resource("b", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    make_dataset(tmp_path / "left", resources, {"a": df, "b": df})
    make_dataset(tmp_path / "right", resources, {"a": df, "b": df})

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
    assert "Identical: 2  Changed: 0  Error: 0" in result.output


def test_no_table_name_failed_comparison_exits_two(
    tmp_path: Path, pk_resource, make_dataset
):
    """A table that fails to compare is reported, and doesn't stop the others."""
    resources = [pk_resource("a", ["x"]), pk_resource("b", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    make_dataset(tmp_path / "left", resources, {"a": df, "b": df})
    make_dataset(tmp_path / "right", resources, {"a": df, "b": df})
    # Corrupt one table on the right so that comparing it raises.
    (tmp_path / "right" / "a.parquet").write_bytes(b"not a parquet file")

    result = CliRunner().invoke(
        main,
        [
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--loglevel",
            "CRITICAL",  # pytest live logging clobbers CliRunner's stdout
            "--output-path",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Identical: 1  Changed: 0  Error: 1" in result.output
    assert "Tables with errors: 1\n  a\n" in result.output
    report = _load_report(tmp_path / "out")
    assert report["success"] is False
    assert report["error"] is None  # only individual tables failed
    assert report["summary"]["failed_tables"] == ["a"]
    assert report["tables"]["a"]["success"] is False
    assert report["tables"]["b"]["success"] is True


def test_no_table_name_no_tables_in_common_exits_two(
    tmp_path: Path, no_pk_resource, make_dataset
):
    df = pl.DataFrame({"x": [1]})
    make_dataset(tmp_path / "left", [no_pk_resource("a")], {"a": df})
    make_dataset(tmp_path / "right", [no_pk_resource("b")], {"b": df})
    output_path = tmp_path / "out"

    result = CliRunner().invoke(
        main,
        [
            "--left",
            str(tmp_path / "left"),
            "--right",
            str(tmp_path / "right"),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 2
    assert "No tables found in both" in result.output
    # The report still records what went wrong.
    report = _load_report(output_path)
    assert report["success"] is False
    assert "No tables found in both" in report["error"]
    assert report["tables"] == {}


def test_summary_shows_total_size_and_change(tmp_path: Path, two_table_args):
    result = CliRunner().invoke(main, ["same_table", "changed_table", *two_table_args])

    assert re.search(r"Total size: +[\d.]+ K?B left, [\d.]+ K?B right", result.output)
    assert re.search(r"Size change: +[+-][\d.]+ K?B [+-][\d.]+%", result.output)


def test_right_table_requires_a_table_name(tmp_path: Path, all_tables_datasets):
    left, right = all_tables_datasets
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


def test_multiple_table_names_compares_only_those_tables(
    tmp_path: Path, all_tables_datasets
):
    left, right = all_tables_datasets
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
    assert list(_load_report(output_path)["tables"]) == [
        "changed_table",
        "same_table",
    ]
    assert "Comparing 2 tables between" in result.output
    assert "Identical: 1  Changed: 1  Error: 0" in result.output
    # Tables that weren't asked for aren't listed as one-sided.
    assert "Tables removed" not in result.output
    assert "Tables added" not in result.output


def test_multiple_table_names_including_a_missing_table_exits_two(
    tmp_path: Path, all_tables_datasets
):
    left, right = all_tables_datasets
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
            "--loglevel",
            "CRITICAL",  # pytest live logging clobbers CliRunner's stdout
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "Identical: 1  Changed: 0  Error: 1" in result.output
    assert "Tables with errors: 1\n  left_only_table\n" in result.output
    assert "same_table" in _load_report(output_path)["tables"]


@pytest.fixture
def two_table_args(tmp_path: Path, all_tables_datasets) -> list[str]:
    left, right = all_tables_datasets
    return ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out")]


def test_short_flags(tmp_path: Path, two_table_args):
    result = CliRunner().invoke(main, ["changed_table", *two_table_args])

    assert result.exit_code == 1, result.output
    assert "changed_table" in _load_report(tmp_path / "out")["tables"]


def test_color_flag_forces_ansi_output(tmp_path: Path, two_table_args):
    result = CliRunner().invoke(
        main, ["same_table", "changed_table", *two_table_args, "--color"]
    )

    assert result.exit_code == 1, result.output
    assert "\x1b[" in result.output


def test_no_color_flag_and_non_tty_default_have_no_ansi_output(
    tmp_path: Path, two_table_args
):
    args = ["same_table", "changed_table", *two_table_args]

    # CliRunner's stdout isn't a terminal, so that's the default.
    default = CliRunner().invoke(main, args)
    forced_off = CliRunner().invoke(main, [*args, "--no-color"])

    assert "\x1b[" not in default.output
    assert "\x1b[" not in forced_off.output
    assert "[IDENTICAL]" in default.output


def test_progress_shows_sub_second_runtimes_with_millisecond_precision(
    tmp_path: Path, two_table_args
):
    result = CliRunner().invoke(main, ["same_table", "changed_table", *two_table_args])

    assert re.search(
        r"\[1/2\]  \[IDENTICAL\] .* \d+\.\d{3}s  same_table", result.output
    )


def _load_report(output_path: Path) -> dict:
    return json.loads((output_path / REPORT_FILENAME).read_text())


def _plain(line: str) -> str:
    """Strip ANSI escape codes."""
    return re.sub(r"\x1b\[[0-9;]*m", "", line)


def test_cli_row_shows_status_and_changes(tmp_path: Path, pk_resource, make_dataset):
    """End to end, a table with a primary key shows +added/changed/-removed."""
    resources = [pk_resource("t", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})},
    )
    make_dataset(
        tmp_path / "right",
        resources,
        {"t": pl.DataFrame({"x": [2, 3, 4, 5], "y": ["b", "changed", "d", "e"]})},
    )

    result = CliRunner().invoke(
        main,
        [
            "-l",
            str(tmp_path / "left"),
            "-r",
            str(tmp_path / "right"),
            "-o",
            str(tmp_path / "o"),
        ],
    )

    assert result.exit_code == 1, result.output
    assert "[CHANGED]" in result.output
    assert "+2/1/-1" in result.output


def test_cli_shows_header_and_summary_with_paths_time_and_memory(
    tmp_path: Path, all_tables_datasets
):
    left, right = all_tables_datasets

    result = CliRunner().invoke(
        main, ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out")]
    )

    assert result.exit_code == 1, result.output
    assert "STATUS" in result.output
    assert "TABLE" in result.output
    assert f"Left:            {left}" in result.output
    assert f"Right:           {right}" in result.output
    assert re.search(r"Elapsed: +\d+\.\d{3}s", result.output)
    # The peak is the highest of any single table's, and names that table.
    assert re.search(
        r"Peak memory: +\d+\.\d [KMG]?B \((changed_table|same_table)\)", result.output
    )
    # Totals: 2 rows in each of the two tables on the left, and the one changed
    # table's 2 changed rows (the same key, but a different value for y).
    assert "Total rows:      4 left, 4 right (2 tables)" in result.output
    assert "Row changes:     +0/2/-0" in result.output
    assert "% of left rows:  +0%/50.00%/-0%" in result.output


def test_cli_shows_added_and_removed_columns(tmp_path: Path, pk_resource, make_dataset):
    resources = [pk_resource("t", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "old": [1, 2]})},
    )
    make_dataset(
        tmp_path / "right",
        resources,
        {
            "t": pl.DataFrame(
                {"x": [1, 2], "y": ["a", "b"], "new1": [1, 2], "new2": [3, 4]}
            )
        },
    )

    result = CliRunner().invoke(
        main,
        [
            "-l",
            str(tmp_path / "left"),
            "-r",
            str(tmp_path / "right"),
            "-o",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code == 1, result.output
    # The rows are unchanged, but two columns were added and one removed.
    assert "[CHANGED]" in result.output
    # The left table had three columns, and the row counts are unchanged.
    assert re.search(r"\b3 +\+2/0/-1 +[\d,]+ +\+0/0/-0", result.output)


def test_summary_totals_count_uncompared_tables(tmp_path: Path, all_tables_datasets):
    left, right = all_tables_datasets

    result = CliRunner().invoke(
        main,
        [
            "same_table",
            "changed_table",
            "-l",
            str(left),
            "-r",
            str(right),
            "-o",
            str(tmp_path / "out"),
            "--max-compare-rows",
            "1",
        ],
    )

    # Neither table was row-compared, but their rows count towards the total.
    assert "Total rows:      4 left, 4 right (2 tables)" in result.output
    assert "Row changes:     +0/0/-0" in result.output
    assert "Not compared:    2 tables (4 left rows)" in result.output


def test_set_log_level_hides_lower_severities_and_restores(caplog):
    logger = get_logger("pudl_diff_test")
    before = logging.getLogger("pudl_diff").level

    restore = _set_log_level("ERROR")
    logger.warning("hidden warning")
    logger.error("shown error")
    restore()
    logger.warning("visible again")

    messages = [record.getMessage() for record in caplog.records]
    assert "hidden warning" not in messages
    assert "shown error" in messages
    assert "visible again" in messages
    assert logging.getLogger("pudl_diff").level == before


def test_cli_leaves_logging_as_it_found_it(tmp_path: Path, all_tables_datasets):
    left, right = all_tables_datasets
    before = logging.getLogger("pudl_diff").level

    CliRunner().invoke(
        main,
        ["same_table", "-l", str(left), "-r", str(right), "-o", str(tmp_path / "o")],
    )

    assert logging.getLogger("pudl_diff").level == before


def test_cli_shows_dtype_changes_in_the_columns_cell(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("t", ["x"])]
    make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2], "y": [1, 2]})},
    )
    make_dataset(
        tmp_path / "right",
        resources,
        {"t": pl.DataFrame({"x": [1, 2], "y": [1.0, 2.0]})},
    )

    result = CliRunner().invoke(
        main,
        [
            "-l",
            str(tmp_path / "left"),
            "-r",
            str(tmp_path / "right"),
            "-o",
            str(tmp_path / "out"),
        ],
    )

    # No columns were added or removed, but one changed dtype, so the table
    # counts as changed even though none of its values differ.
    assert result.exit_code == 1, result.output
    assert "[CHANGED]" in result.output
    assert re.search(r"\+0/1/-0 +[\d,]+ +\+0/0/-0", result.output)


@pytest.fixture
def schema_change_datasets(
    tmp_path: Path, pk_resource, make_dataset
) -> tuple[Path, Path]:
    """Two datasets sharing four tables: one with added and removed columns, one
    with a changed dtype, one with both, and one identical; plus a table removed
    from and a table added to the right dataset."""
    resources = [
        pk_resource(name, ["x"])
        for name in ["cols", "dtype", "both", "same", "removed", "added"]
    ]
    base = pl.DataFrame({"x": [1, 2], "y": [1, 2], "old": [1, 2]})
    left = tmp_path / "left"
    right = tmp_path / "right"
    make_dataset(
        left,
        resources,
        {"cols": base, "dtype": base, "both": base, "same": base, "removed": base},
    )
    make_dataset(
        right,
        resources,
        {
            # +2 columns, -1 column
            "cols": base.drop("old").with_columns(new1=pl.lit(1), new2=pl.lit(2)),
            # one column changes dtype
            "dtype": base.with_columns(pl.col("y").cast(pl.Float64)),
            # +1 column, -1 column, and one dtype change
            "both": base.drop("old").with_columns(
                pl.col("y").cast(pl.Float64), z=pl.lit(1)
            ),
            "same": base,
            "added": base,
        },
    )
    return left, right


def test_summary_totals_schema_changes(tmp_path: Path, schema_change_datasets):
    left, right = schema_change_datasets

    result = CliRunner().invoke(
        main, ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out")]
    )

    assert result.exit_code == 1, result.output
    # cols: +2/-1; dtype: ~1; both: +1/~1/-1. Three of the four tables changed.
    assert "Column changes:  +3/2/-2" in result.output
    assert "Schema changes:  3 tables" in result.output
    # The tables whose schema changed are listed, one per line, with their own
    # column changes. The unchanged table isn't.
    assert (
        "Schema changes:  3 tables\n  both   +1/1/-1\n  cols   +2/0/-1\n  dtype  +0/1/-0\n"
        in result.output
    )
    assert "  same " not in result.output
    assert "Tables removed (only in left): 1\n  removed\n" in result.output
    assert "Tables added (only in right): 1\n  added\n" in result.output


def test_summary_schema_totals_are_gray_when_nothing_changed(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("t", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    make_dataset(tmp_path / "left", resources, {"t": df})
    make_dataset(tmp_path / "right", resources, {"t": df})

    result = CliRunner().invoke(
        main,
        [
            "-l",
            str(tmp_path / "left"),
            "-r",
            str(tmp_path / "right"),
            "-o",
            str(tmp_path / "out"),
            "--color",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Schema changes:  0 tables" in _plain(result.output)
    assert "Tables removed" not in result.output
    assert "Tables added" not in result.output


def test_summary_schema_and_table_list_colors(tmp_path: Path, schema_change_datasets):
    left, right = schema_change_datasets

    result = CliRunner().invoke(
        main,
        ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out"), "--color"],
    )

    summary = result.output[result.output.index("Column changes:") :]
    cyan, hot_pink, magenta = "\x1b[36m", "\x1b[38;5;205m", "\x1b[35m"
    column_line = summary.splitlines()[0]
    assert cyan in column_line
    assert hot_pink in column_line
    assert magenta in column_line
    # Each listed table's own column changes are colored the same way.
    list_line = next(line for line in summary.splitlines() if "  cols " in line)
    assert cyan in list_line
    assert magenta in list_line
    # The headings of the table lists are bold, not colored.
    bold = "\x1b[1m"
    for heading in ["Tables removed (only in left):", "Tables added (only in right):"]:
        assert f"{bold}{heading}" in summary
    heading_lines = [
        line for line in summary.splitlines() if line.startswith(f"{bold}Tables")
    ]
    assert len(heading_lines) == 2
    assert not any(color in line for line in heading_lines for color in (cyan, magenta))


def test_summary_descriptors_are_bold(tmp_path: Path, all_tables_datasets):
    left, right = all_tables_datasets

    result = CliRunner().invoke(
        main,
        ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out"), "--color"],
    )

    bold = "\x1b[1m"
    for descriptor in [
        "Left:",
        "Right:",
        "Elapsed:",
        "Peak memory:",
        "Total rows:",
        "Row changes:",
        "% of left rows:",
        "Column changes:",
        "Schema changes:",
        "Report written to",
    ]:
        assert f"{bold}{descriptor}" in result.output, descriptor
    # The values aren't bold, and stay aligned once the styling is stripped.
    assert re.search(r"^Left: {12}\S", _plain(result.output), re.MULTILINE)
    assert re.search(r"^Elapsed: {9}\S", _plain(result.output), re.MULTILINE)


def test_summary_headline_is_bold_and_separated_from_the_rest(
    tmp_path: Path, all_tables_datasets
):
    left, right = all_tables_datasets

    result = CliRunner().invoke(
        main,
        ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out"), "--color"],
    )

    bold, reset = "\x1b[1m", "\x1b[0m"
    assert f"\x1b[32m{bold}Identical{reset}: {bold}1{reset}" in result.output
    assert f"\x1b[33m{bold}Changed{reset}: {bold}1{reset}" in result.output
    assert f"\x1b[31m{bold}Error{reset}: {bold}0{reset}" in result.output
    # A line under the headline sets it apart from the rest of the summary.
    lines = _plain(result.output).splitlines()
    headline = lines.index("Identical: 1  Changed: 1  Error: 0")
    assert set(lines[headline + 1]) == {"─"}
    assert lines[headline + 2].startswith("Left:")


def test_report_paths_do_not_depend_on_the_working_directory_and_it_can_be_moved(
    tmp_path: Path, pk_resource, make_dataset
):
    resources = [pk_resource("t", ["x"])]
    make_dataset(tmp_path / "left", resources, {"t": pl.DataFrame({"x": [1, 2]})})
    make_dataset(tmp_path / "right", resources, {"t": pl.DataFrame({"x": [2, 3]})})

    with contextlib.chdir(tmp_path):
        result = CliRunner().invoke(
            main, ["-l", "left", "-r", "right", "-o", "diffs/here", "t"]
        )
    assert result.exit_code == 1, result.output

    # Everything the report refers to is found from the report itself.
    report_dir = tmp_path / "diffs" / "here"
    report = _load_report(report_dir)
    assert Path(report["left_dataset"]["root"]) == (tmp_path / "left").resolve()
    assert Path(report["right_dataset"]["root"]) == (tmp_path / "right").resolve()
    table = report["tables"]["t"]
    assert Path(table["left_table_path"]).is_file()
    assert Path(table["right_table_path"]).is_file()
    left_only = table["row_diff"]["left_only_parquet"]["path"]
    assert left_only == "t_left_only.parquet"
    assert (report_dir / left_only).is_file()

    # ...and moving the report's directory doesn't break its links to the outputs.
    moved = tmp_path / "elsewhere" / "moved"
    moved.parent.mkdir()
    shutil.move(report_dir, moved)
    moved_report = _load_report(moved)
    for side in ("left_only_parquet", "right_only_parquet"):
        assert (moved / moved_report["tables"]["t"]["row_diff"][side]["path"]).is_file()


def _without_intro_and_footer(output: str) -> list[str]:
    """The lines of output that don't depend on how the report was made."""
    lines = output.strip().splitlines()
    return lines[1:-1]


@pytest.mark.parametrize("tables", [["same_table", "changed_table"], ["changed_table"]])
def test_from_report_shows_what_the_comparison_showed(
    tmp_path: Path, two_table_args, tables
):
    original = CliRunner().invoke(main, [*tables, *two_table_args])
    assert original.exit_code == 1, original.output

    replay = CliRunner().invoke(main, ["--from-report", str(tmp_path / "out")])

    assert replay.exit_code == original.exit_code, replay.output
    assert _without_intro_and_footer(replay.output) == _without_intro_and_footer(
        original.output
    )
    assert "Report of" in replay.output.splitlines()[0]
    assert f"Report read from {tmp_path / 'out' / REPORT_FILENAME}" in replay.output


def test_from_report_accepts_the_report_file_itself(tmp_path: Path, two_table_args):
    CliRunner().invoke(main, ["same_table", *two_table_args])

    replay = CliRunner().invoke(
        main, ["--from-report", str(tmp_path / "out" / REPORT_FILENAME)]
    )

    assert replay.exit_code == 0, replay.output
    assert "[IDENTICAL]" in replay.output


def test_from_report_does_not_compare_or_write_anything(
    tmp_path: Path, two_table_args, mocker
):
    CliRunner().invoke(main, ["same_table", *two_table_args])
    run = mocker.patch("pudl_diff.cli.run_dataset_diff")
    before = sorted(p.name for p in (tmp_path / "out").iterdir())

    CliRunner().invoke(main, ["--from-report", str(tmp_path / "out")])

    run.assert_not_called()
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == before


def test_from_report_shows_a_failed_run(tmp_path: Path, pk_resource, make_dataset):
    make_dataset(tmp_path / "left", [pk_resource("a", ["x"])], {})
    make_dataset(tmp_path / "right", [pk_resource("b", ["x"])], {})
    args = ["-l", str(tmp_path / "left"), "-r", str(tmp_path / "right")]
    CliRunner().invoke(main, [*args, "-o", str(tmp_path / "out")])

    replay = CliRunner().invoke(main, ["--from-report", str(tmp_path / "out")])

    assert replay.exit_code == 2
    assert "No tables found in both" in replay.output


def test_from_report_rejects_options_that_control_a_comparison(
    tmp_path: Path, two_table_args
):
    CliRunner().invoke(main, ["same_table", *two_table_args])

    replay = CliRunner().invoke(
        main,
        ["--from-report", str(tmp_path / "out"), "same_table", "--rtol", "0.1"],
    )

    assert replay.exit_code == 2
    assert "table_names" in replay.output
    assert "--rtol" in replay.output


def test_from_report_rejects_something_that_isnt_a_report(tmp_path: Path):
    (tmp_path / "junk.json").write_text('{"not": "a report"}')

    replay = CliRunner().invoke(main, ["--from-report", str(tmp_path / "junk.json")])

    assert replay.exit_code == 1
    assert "Couldn't read a report" in replay.output


def test_missing_default_right_dataset_is_a_usage_error(tmp_path: Path, mocker):
    mocker.patch(
        "pudl_diff.cli.default_right_root",
        side_effect=RuntimeError("give the right dataset's root explicitly"),
    )

    result = CliRunner().invoke(main, ["--left", str(tmp_path)])

    assert result.exit_code == 2
    assert "give the right dataset's root explicitly" in result.output


def test_set_log_level_keeps_the_handlers_the_application_configured():
    logger = logging.getLogger("pudl_diff")
    handler = logging.NullHandler()
    logger.addHandler(handler)
    try:
        restore = _set_log_level("ERROR")
        assert handler.level == logging.ERROR
        assert logger.handlers == [handler]  # no stderr handler was added
        restore()
        assert logger.handlers == [handler]
    finally:
        logger.removeHandler(handler)
