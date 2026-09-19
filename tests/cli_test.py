"""Unit tests for the pudl_diff script."""

import json
import logging
import re
from pathlib import Path

import polars as pl
from click.testing import CliRunner

from pudl.logging_helpers import get_logger
from pudl.scripts.pudl_diff import (
    REPORT_FILENAME,
    _set_log_level,
    main,
)
from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import TableOutcome
from pudl.validate.diff.formatting import (
    format_duration,
    format_percent,
    format_signed_percent,
)
from pudl.validate.diff.runner import run_dataset_diff
from pudl.validate.diff.terminal import TerminalProgress, format_header, format_outcome


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
    report = _load_report(output_path)["tables"]["table_with_pk"]
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
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
    report = _load_report(output_path)["tables"]["table_without_pk"]
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
    report = _load_report(output_path)["tables"]["nonexistent_table"]
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
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
    report = _load_report(output_path)["tables"]["table_with_pk"]
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
    report = _load_report(output_path)["tables"]["table_without_pk"]
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
    # Everything goes in a single report, however many tables are compared.
    assert [p.name for p in output_path.glob("*.json")] == [REPORT_FILENAME]
    report = _load_report(output_path)
    assert report["schema_version"] == "1.0.0"
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
    assert "Identical: 2  Changed: 0  Error: 0" in result.output


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


def test_no_table_name_no_tables_in_common_exits_two(tmp_path: Path):
    df = pl.DataFrame({"x": [1]})
    _make_dataset(tmp_path / "left", [_no_pk_resource("a")], {"a": df})
    _make_dataset(tmp_path / "right", [_no_pk_resource("b")], {"b": df})
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


def test_summary_shows_total_size_and_change(tmp_path: Path):
    result = CliRunner().invoke(
        main, ["same_table", "changed_table", *_two_table_args(tmp_path)]
    )

    assert re.search(r"Total size: +[\d.]+ K?B left, [\d.]+ K?B right", result.output)
    assert re.search(r"Size change: +[+-][\d.]+ K?B [+-][\d.]+%", result.output)


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
    assert list(_load_report(output_path)["tables"]) == [
        "changed_table",
        "same_table",
    ]
    assert "Comparing 2 tables between" in result.output
    assert "Identical: 1  Changed: 1  Error: 0" in result.output
    # Tables that weren't asked for aren't listed as one-sided.
    assert "Tables removed" not in result.output
    assert "Tables added" not in result.output


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


def _two_table_args(tmp_path: Path) -> list[str]:
    left, right = _all_tables_datasets(tmp_path)
    return ["-l", str(left), "-r", str(right), "-o", str(tmp_path / "out")]


def test_short_flags(tmp_path: Path):
    result = CliRunner().invoke(main, ["changed_table", *_two_table_args(tmp_path)])

    assert result.exit_code == 1, result.output
    assert "changed_table" in _load_report(tmp_path / "out")["tables"]


def test_color_flag_forces_ansi_output(tmp_path: Path):
    result = CliRunner().invoke(
        main, ["same_table", "changed_table", *_two_table_args(tmp_path), "--color"]
    )

    assert result.exit_code == 1, result.output
    assert "\x1b[" in result.output


def test_no_color_flag_and_non_tty_default_have_no_ansi_output(tmp_path: Path):
    args = ["same_table", "changed_table", *_two_table_args(tmp_path)]

    # CliRunner's stdout isn't a terminal, so that's the default.
    default = CliRunner().invoke(main, args)
    forced_off = CliRunner().invoke(main, [*args, "--no-color"])

    assert "\x1b[" not in default.output
    assert "\x1b[" not in forced_off.output
    assert "[IDENTICAL]" in default.output


def test_progress_shows_sub_second_runtimes_with_millisecond_precision(tmp_path: Path):
    result = CliRunner().invoke(
        main, ["same_table", "changed_table", *_two_table_args(tmp_path)]
    )

    assert re.search(
        r"\[1/2\]  \[IDENTICAL\] .* \d+\.\d{3}s  same_table", result.output
    )


def _load_report(output_path: Path) -> dict:
    return json.loads((output_path / REPORT_FILENAME).read_text())


def table_outcome(
    exit_code: int,
    *,
    added: int | None = None,
    changed: int | None = None,
    removed: int | None = None,
    skipped_reason: str | None = None,
    has_primary_key: bool | None = None,
    sizes: table_report.SizeComparison | None = None,
    **kwargs,
) -> TableOutcome:
    return TableOutcome(
        table_name="some_table",
        exit_code=exit_code,
        elapsed_seconds=1.5,
        error=None,
        rows=table_report.RowChanges(
            added=added,
            changed=changed,
            removed=removed,
            skipped_reason=skipped_reason,
            has_primary_key=has_primary_key,
        ),
        sizes=sizes or table_report.SizeComparison(),
        **kwargs,
    )


def _plain(line: str) -> str:
    """Strip ANSI escape codes."""
    return re.sub(r"\x1b\[[0-9;]*m", "", line)


def test_format_outcome_table_with_primary_key():
    line = _plain(
        format_outcome(table_outcome(1, added=1_234_567, changed=221, removed=764))
    )
    assert line.startswith("[CHANGED]")
    assert "+1,234,567/221/-764" in line
    assert line.endswith("1.500s  some_table")


def test_format_outcome_table_without_primary_key_has_no_middle_count():
    line = _plain(format_outcome(table_outcome(1, added=50, removed=30)))
    assert "+50/-30" in line


def test_format_outcome_identical_and_progress_prefix():
    line = _plain(
        format_outcome(table_outcome(0, added=0, changed=0, removed=0), "[ 3/378]")
    )
    assert line.startswith("[ 3/378]  [IDENTICAL]")
    assert "+0/0/-0" in line


def test_format_outcome_skipped_and_error_messages():
    skipped = _plain(format_outcome(table_outcome(1, skipped_reason="too_many_rows")))
    assert "[CHANGED]" in skipped
    assert "row diff skipped: too many rows" in skipped

    error = _plain(format_outcome(table_outcome(2)))
    assert "[ERROR]" in error
    assert "comparison failed" in error


def test_format_outcome_column_changes():
    line = _plain(
        format_outcome(
            table_outcome(
                1,
                added=0,
                changed=0,
                removed=0,
                columns_added=2,
                columns_removed=1,
                dtypes_changed=3,
            )
        )
    )
    assert "+2/3/-1" in line
    assert line.index("+2/3/-1") < line.index("+0/0/-0")  # columns come before rows
    assert "(dtypes changed)" not in line


def test_format_outcome_column_colors():
    gray, cyan, hot_pink, magenta = "\x1b[90m", "\x1b[36m", "\x1b[38;5;205m", "\x1b[35m"
    kwargs = {"added": 0, "changed": 0, "removed": 0}

    changed = format_outcome(
        table_outcome(1, columns_added=2, columns_removed=1, dtypes_changed=3, **kwargs)
    )
    assert cyan in changed
    assert hot_pink in changed
    assert magenta in changed

    # Zero column counts are gray, like zero row counts.
    unchanged = format_outcome(
        table_outcome(0, columns_added=0, columns_removed=0, **kwargs)
    )
    assert cyan not in unchanged
    assert hot_pink not in unchanged
    assert magenta not in unchanged
    assert gray in unchanged


def test_format_outcome_error_has_no_column_counts():
    line = _plain(format_outcome(table_outcome(2)))
    assert "+0/0/-0" not in line


def test_format_outcome_left_columns():
    kwargs = {"added": 0, "changed": 0, "removed": 0}
    line = _plain(
        format_outcome(
            table_outcome(
                0,
                columns_added=1,
                columns_removed=0,
                dtypes_changed=0,
                left_columns=1_234,
                **kwargs,
            )
        )
    )
    assert re.search(r"\b1,234 +\+1/0/-0\b", line)


def test_format_header_names_each_column():
    header = _plain(format_header(len("[3/378]")))
    for heading in [
        "STATUS",
        "KEY",
        "LEFT COLS",
        "COLS +add/~chg/-del",
        "LEFT ROWS",
        "ROWS +add/~chg/-del",
        "% OF LEFT ROWS",
        "TIME",
        "TABLE",
    ]:
        assert heading in header
    # The headings line up with the values in the rows below.
    row = _plain(
        format_outcome(
            table_outcome(
                0,
                added=0,
                changed=0,
                removed=0,
                columns_added=1,
                columns_removed=2,
                dtypes_changed=3,
                has_primary_key=True,
                left_columns=42,
                left_rows=1_234,
            ),
            "[3/378]",
        )
    )
    assert header.index("STATUS") == row.index("[IDENTICAL]")
    assert header.index("KEY") == row.index("PK")
    # Column counts are right-aligned too.
    assert header.index("LEFT COLS") + len("LEFT COLS") == row.index("42") + 2
    assert header.index("COLS +add") == row.index("+1/3/-2")
    # Row counts are right-aligned.
    assert header.index("LEFT ROWS") + len("LEFT ROWS") == row.index("1,234") + 5
    assert header.index("ROWS +add") == row.index("+0/0/-0")
    assert header.index("% OF LEFT ROWS") == row.index("+0%/0%/-0%")
    assert header.index("TABLE") == row.index("some_table")


def test_format_outcome_key_and_left_rows():
    pk = _plain(
        format_outcome(
            table_outcome(
                1, added=1, changed=1, removed=1, has_primary_key=True, left_rows=1_500
            )
        )
    )
    assert re.search(r"\bPK\b.*\b1,500\b", pk)
    no_pk = _plain(
        format_outcome(
            table_outcome(1, added=1, removed=1, has_primary_key=False, left_rows=1_500)
        )
    )
    assert "no-PK" in no_pk
    # An error means we don't know either.
    error = _plain(format_outcome(table_outcome(2)))
    assert "PK" not in error


def test_format_outcome_percentages_are_relative_to_left_rows():
    line = _plain(
        format_outcome(
            table_outcome(
                1,
                added=50,
                changed=221,
                removed=764,
                has_primary_key=True,
                left_rows=10_000,
            )
        )
    )
    assert "+50/221/-764" in line
    assert "+0.50%/2.21%/-7.64%" in line


def test_format_outcome_skipped_row_diff_still_shows_left_rows_but_no_percentages():
    line = _plain(
        format_outcome(
            table_outcome(
                1, skipped_reason="too_many_rows", has_primary_key=True, left_rows=99
            )
        )
    )
    assert "row diff skipped: too many rows" in line
    assert " 99 " in line
    assert "%" not in line.replace("% OF", "")


def test_format_percent():
    assert format_percent(0, 100) == "0%"
    assert format_percent(0, 0) == "0%"
    assert format_percent(1, 0) == "n/a"
    assert format_percent(1, 100) == "1.00%"
    assert format_percent(1, 1_000_000) == "<0.01%"
    assert format_percent(250, 100) == "250%"
    assert format_percent(12_345, 100_000) == "12.35%"


def test_format_duration():
    assert format_duration(0.0432) == "0.043s"
    assert format_duration(38.0) == "38.000s"
    assert format_duration(125.4) == "2m 05.4s"
    assert format_duration(3723.0) == "1h 02m 03s"


def test_format_signed_percent():
    assert format_signed_percent(0) == "0%"
    assert format_signed_percent(1.234) == "+1.23%"
    assert format_signed_percent(-1.234) == "-1.23%"
    assert format_signed_percent(0.001) == "+<0.01%"
    assert format_signed_percent(-0.001) == "-<0.01%"


def test_format_outcome_sizes_and_their_colors():
    blue, orange, gray = "\x1b[38;5;39m", "\x1b[38;5;208m", "\x1b[90m"

    def line(left: int, right: int) -> str:
        sizes = table_report.SizeComparison(
            left_table_bytes=left, right_table_bytes=right
        )
        return format_outcome(table_outcome(1, sizes=sizes))

    grew = line(10_000_000, 10_500_000)
    assert "10.0 MB" in grew
    assert "10.5 MB" in grew
    assert f"{blue}+500.0 KB\x1b[0m" in grew
    assert f"{blue}+5.00%\x1b[0m" in grew

    shrank = line(10_000_000, 9_000_000)
    assert f"{orange}-1.0 MB\x1b[0m" in shrank
    assert f"{orange}-10.00%\x1b[0m" in shrank

    same = line(1_000, 1_000)
    assert f"{gray}0 B\x1b[0m" in same
    assert f"{gray}0%\x1b[0m" in same


def test_format_outcome_unknown_sizes_are_blank():
    line = _plain(format_outcome(table_outcome(2)))
    assert "MB" not in line
    assert "%" not in line.replace("% OF", "")


def test_format_header_names_size_columns():
    header = _plain(format_header())
    for heading in ["LEFT SIZE", "RIGHT SIZE", "SIZE CHANGE", "% SIZE"]:
        assert heading in header


def test_format_outcome_colors():
    gray, green, yellow, red = "\x1b[90m", "\x1b[32m", "\x1b[33m", "\x1b[31m"

    mixed = format_outcome(table_outcome(1, added=5, changed=6, removed=7))
    for code in (green, yellow, red):
        assert code in mixed

    # Zero counts are gray rather than colored.
    zeros = format_outcome(table_outcome(0, added=0, changed=0, removed=0))
    assert gray in zeros
    assert yellow not in zeros
    assert red not in zeros
    assert f"{green}+0" not in zeros


def test_cli_row_shows_status_and_changes(tmp_path: Path):
    """End to end, a table with a primary key shows +added/changed/-removed."""
    resources = [_pk_resource("t", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})},
    )
    _make_dataset(
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


def test_cli_shows_header_and_summary_with_paths_time_and_memory(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)

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


def test_cli_shows_added_and_removed_columns(tmp_path: Path):
    resources = [_pk_resource("t", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2], "y": ["a", "b"], "old": [1, 2]})},
    )
    _make_dataset(
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


def test_summary_totals_count_uncompared_tables(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)

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
    logger = get_logger("pudl.scripts.pudl_diff_test")
    before = logging.getLogger("catalystcoop").level

    restore = _set_log_level("ERROR")
    logger.warning("hidden warning")
    logger.error("shown error")
    restore()
    logger.warning("visible again")

    messages = [record.getMessage() for record in caplog.records]
    assert "hidden warning" not in messages
    assert "shown error" in messages
    assert "visible again" in messages
    assert logging.getLogger("catalystcoop").level == before


def test_cli_leaves_logging_as_it_found_it(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)
    before = logging.getLogger("catalystcoop").level

    CliRunner().invoke(
        main,
        ["same_table", "-l", str(left), "-r", str(right), "-o", str(tmp_path / "o")],
    )

    assert logging.getLogger("catalystcoop").level == before


def test_cli_shows_dtype_changes_in_the_columns_cell(tmp_path: Path):
    resources = [_pk_resource("t", ["x"])]
    _make_dataset(
        tmp_path / "left",
        resources,
        {"t": pl.DataFrame({"x": [1, 2], "y": [1, 2]})},
    )
    _make_dataset(
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


def _schema_change_datasets(tmp_path: Path) -> tuple[Path, Path]:
    """Two datasets sharing four tables: one with added and removed columns, one
    with a changed dtype, one with both, and one identical; plus a table removed
    from and a table added to the right dataset."""
    resources = [
        _pk_resource(name, ["x"])
        for name in ["cols", "dtype", "both", "same", "removed", "added"]
    ]
    base = pl.DataFrame({"x": [1, 2], "y": [1, 2], "old": [1, 2]})
    left = tmp_path / "left"
    right = tmp_path / "right"
    _make_dataset(
        left,
        resources,
        {"cols": base, "dtype": base, "both": base, "same": base, "removed": base},
    )
    _make_dataset(
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


def test_summary_totals_schema_changes(tmp_path: Path):
    left, right = _schema_change_datasets(tmp_path)

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


def test_summary_schema_totals_are_gray_when_nothing_changed(tmp_path: Path):
    resources = [_pk_resource("t", ["x"])]
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    _make_dataset(tmp_path / "left", resources, {"t": df})
    _make_dataset(tmp_path / "right", resources, {"t": df})

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


def test_summary_schema_and_table_list_colors(tmp_path: Path):
    left, right = _schema_change_datasets(tmp_path)

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


def test_summary_descriptors_are_bold(tmp_path: Path):
    left, right = _all_tables_datasets(tmp_path)

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


def test_terminal_progress_prints_a_numbered_line_per_table(tmp_path: Path, capsys):
    resources = [_pk_resource("table_a", ["x"]), _pk_resource("table_b", ["x"])]
    tables = {
        name: pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        for name in ("table_a", "table_b")
    }
    _make_dataset(tmp_path / "left", resources, tables)
    _make_dataset(tmp_path / "right", resources, tables)
    progress = TerminalProgress("left", "right", explicit=False, show_progress=True)

    run_dataset_diff(
        PudlDiffDataset(tmp_path / "left"),
        PudlDiffDataset(tmp_path / "right"),
        tmp_path / "out",
        on_tables_resolved=progress.tables_resolved,
        on_table_compared=progress.table_compared,
    )

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "Comparing 2 tables present in both 'left' and 'right'."
    assert lines[1].split()[0] == "STATUS"
    assert re.match(r"\[1/2\]\s+\[IDENTICAL\].*table_a$", lines[2])
    assert re.match(r"\[2/2\]\s+\[IDENTICAL\].*table_b$", lines[3])
    assert [o.table_name for o in progress.outcomes] == ["table_a", "table_b"]
