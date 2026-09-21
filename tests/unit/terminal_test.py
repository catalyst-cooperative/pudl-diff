"""Unit tests for pudl_diff.terminal."""

import re
from pathlib import Path

from pudl_diff import table_report
from pudl_diff.dataset_report import TableOutcome, build_pudl_diff_report
from pudl_diff.runner import run_dataset_diff
from pudl_diff.terminal import (
    TerminalProgress,
    echo_summary,
    format_header,
    format_outcome,
)


def table_outcome(
    exit_code: int,
    *,
    added: int | None = None,
    changed: int | None = None,
    removed: int | None = None,
    skipped_reason: str | None = None,
    has_primary_key: bool | None = None,
    sizes: table_report.SizeComparison | None = None,
    left_rows: int | None = None,
    left_columns: int | None = None,
    columns_added: int | None = None,
    columns_removed: int | None = None,
    dtypes_changed: int = 0,
) -> TableOutcome:
    """A `TableOutcome` with the given exit code and changes, for formatting."""
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
        left_rows=left_rows,
        left_columns=left_columns,
        columns_added=columns_added,
        columns_removed=columns_removed,
        dtypes_changed=dtypes_changed,
    )


def _plain(line: str) -> str:
    """Strip ANSI escape codes."""
    return re.sub(r"\x1b\[[0-9;]*m", "", line)


def test_format_outcome_table_with_primary_key():
    """A table's line has its status, its row changes as `+added/changed/-removed`, and its time.

    Counts have thousands separators. A table with a primary key has all three counts,
    including how many rows were changed, as well as added and removed.
    """
    line = _plain(
        format_outcome(table_outcome(1, added=1_234_567, changed=221, removed=764))
    )
    assert line.startswith("[CHANGED]")
    assert "+1,234,567/221/-764" in line
    assert line.endswith("1.500s  some_table")


def test_format_outcome_table_without_primary_key_has_no_middle_count():
    """Without a primary key, there is no count of changed rows, only added and removed.

    Rows can't be matched to say that one was changed, only that it is or isn't in the other.
    """
    line = _plain(format_outcome(table_outcome(1, added=50, removed=30)))
    assert "+50/-30" in line


def test_format_outcome_identical_and_progress_prefix():
    """Format outcome identical and progress prefix."""
    line = _plain(
        format_outcome(table_outcome(0, added=0, changed=0, removed=0), "[ 3/378]")
    )
    assert line.startswith("[ 3/378]  [IDENTICAL]")
    assert "+0/0/-0" in line


def test_format_outcome_skipped_and_error_messages():
    """A table that wasn't row-compared, or failed, says so where its row changes would be."""
    skipped = _plain(format_outcome(table_outcome(1, skipped_reason="too_many_rows")))
    assert "[CHANGED]" in skipped
    assert "row diff skipped: too many rows" in skipped

    error = _plain(format_outcome(table_outcome(2)))
    assert "[ERROR]" in error
    assert "comparison failed" in error


def test_format_outcome_column_changes():
    """A table's column changes are `+added/changed/-removed`, before its row changes.

    The middle count is the columns whose dtype changed.
    """
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
    """Column changes are cyan for added, hot pink for changed and magenta for removed.

    Zero counts are gray, like zero row counts, so the eye is drawn to what changed.
    """
    gray, cyan, hot_pink, magenta = "\x1b[90m", "\x1b[36m", "\x1b[38;5;205m", "\x1b[35m"
    changed = format_outcome(
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
    assert cyan in changed
    assert hot_pink in changed
    assert magenta in changed

    # Zero column counts are gray, like zero row counts.
    unchanged = format_outcome(
        table_outcome(
            0, added=0, changed=0, removed=0, columns_added=0, columns_removed=0
        )
    )
    assert cyan not in unchanged
    assert hot_pink not in unchanged
    assert magenta not in unchanged
    assert gray in unchanged


def test_format_outcome_error_has_no_column_counts():
    """Format outcome error has no column counts."""
    line = _plain(format_outcome(table_outcome(2)))
    assert "+0/0/-0" not in line


def test_format_outcome_left_columns():
    """The number of columns in the left table is shown before the column changes."""
    line = _plain(
        format_outcome(
            table_outcome(
                0,
                columns_added=1,
                columns_removed=0,
                dtypes_changed=0,
                left_columns=1_234,
                added=0,
                changed=0,
                removed=0,
            )
        )
    )
    assert re.search(r"\b1,234 +\+1/0/-0\b", line)


def test_format_header_has_two_lines_naming_each_column():
    """The header names each column of the table over two lines, aligned with the rows.

    The first line names the groups of columns, and the second the columns themselves,
    three of which have `+add/~chg/-del` counts. Text columns are left-aligned with their
    values, and numbers are right-aligned, which is checked against a real row of output.
    """
    first, second = _plain(format_header(len("[3/378]"))).splitlines()
    for heading in ["LEFT", "COL CHANGES", "ROW CHANGES", "% OF LEFT ROWS"]:
        assert heading in first
    for heading in [
        "STATUS",
        "KEY",
        "COLS",
        "ROWS",
        "TIME",
        "TABLE",
    ]:
        assert heading in second
    assert second.count("+add/~chg/-del") == 3
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
    assert second.index("STATUS") == row.index("[IDENTICAL]")
    assert second.index("KEY") == row.index("PK")
    # Column counts are right-aligned too.
    assert second.index("COLS") + len("COLS") == row.index("42") + 2
    assert first.index("COL CHANGES") == row.index("+1/3/-2")
    # Row counts are right-aligned.
    assert second.index("ROWS") + len("ROWS") == row.index("1,234") + 5
    assert first.index("ROW CHANGES") == row.index("+0/0/-0")
    assert first.index("% OF LEFT ROWS") == row.index("+0%/0%/-0%")
    assert second.index("TABLE") == row.index("some_table")


def test_format_outcome_key_and_left_rows():
    """A row has whether the table has a primary key, and its number of rows in the left table."""
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
    """Changes are also shown as percentages of the number of rows in the left table.

    The left table is the reference that changes are measured from.
    """
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
    """A table that wasn't row-compared has its rows shown, but no percentages of them.

    There are no changes to give as percentages, and showing 0% would say that there
    were none, when the rows were never compared.
    """
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


def test_format_outcome_sizes_and_their_colors():
    """The sizes of the two Parquet files are shown, with their change as an amount and percent.

    A table that grew is green, one that shrank is red, and one whose size didn't change
    is gray, like the row counts.
    """
    green, red, gray = "\x1b[32m", "\x1b[31m", "\x1b[90m"

    def line(left: int, right: int) -> str:
        sizes = table_report.SizeComparison(
            left_table_bytes=left, right_table_bytes=right
        )
        return format_outcome(table_outcome(1, sizes=sizes))

    grew = line(10_000_000, 10_500_000)
    assert "10.0 MB" in grew
    assert "10.5 MB" in grew
    assert f"{green}+500.0 KB\x1b[0m" in grew
    assert f"{green}+5.00%\x1b[0m" in grew

    shrank = line(10_000_000, 9_000_000)
    assert f"{red}-1.0 MB\x1b[0m" in shrank
    assert f"{red}-10.00%\x1b[0m" in shrank

    same = line(1_000, 1_000)
    assert f"{gray}0 B\x1b[0m" in same
    assert f"{gray}0%\x1b[0m" in same


def test_format_outcome_unknown_sizes_are_blank():
    """A table whose comparison failed has no sizes, and its line leaves them blank."""
    line = _plain(format_outcome(table_outcome(2)))
    assert "MB" not in line
    assert "%" not in line.replace("% OF", "")


def test_format_header_names_size_columns():
    """Format header names size columns."""
    header = _plain(format_header())
    for heading in ["LEFT SIZE", "RIGHT SIZE", "SIZE CHANGE", "% SIZE"]:
        assert heading in header


def test_format_outcome_colors():
    """Added, changed and removed rows are green, yellow and red, and zero counts are gray."""
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


def test_terminal_progress_prints_a_numbered_line_per_table(
    tmp_path: Path, write_two_datasets, capsys
):
    """Progress prints what is being compared, then a header, then a numbered line per table.

    The lines come as each table finishes, and the outcomes are kept for the summary.
    """
    tables = {"table_a": ["a", "b"], "table_b": ["a", "b"]}
    left, right = write_two_datasets(tmp_path, tables, tables)
    progress = TerminalProgress("left", "right", explicit=False, show_progress=True)

    run_dataset_diff(
        left,
        right,
        tmp_path / "out",
        on_tables_resolved=progress.tables_resolved,
        on_table_compared=progress.table_compared,
    )

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "Comparing 2 tables present in both 'left' and 'right'."
    # A two-line header
    assert lines[2].split()[0] == "STATUS"
    assert re.match(r"\[1/2\]\s+\[IDENTICAL\].*table_a$", lines[3])
    assert re.match(r"\[2/2\]\s+\[IDENTICAL\].*table_b$", lines[4])
    assert [o.table_name for o in progress.outcomes] == ["table_a", "table_b"]


def test_summary_of_a_report_without_timing_memory_or_sizes(
    tmp_path: Path, write_two_datasets, capsys
):
    """Lines of the summary for information that a report doesn't have are left out.

    A report with no tables has no elapsed time, peak memory or sizes, so those lines aren't
    shown, instead of being shown empty or as zero.
    """
    left, right = write_two_datasets(tmp_path, {"only_left": ["a"]}, {"other": ["a"]})
    report = build_pudl_diff_report(left, right, {})

    echo_summary(report, [], tmp_path / "report.json")

    output = capsys.readouterr().out
    assert "Elapsed:" not in output
    assert "Peak memory:" not in output
    assert "Total size:" not in output
    assert f"Report written to {tmp_path / 'report.json'}" in output
