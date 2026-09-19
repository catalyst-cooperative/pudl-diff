"""Unit tests for pudl.validate.diff.terminal."""

import re
from pathlib import Path

from pudl.validate.diff import table_report
from pudl.validate.diff.dataset_report import TableOutcome
from pudl.validate.diff.runner import run_dataset_diff
from pudl.validate.diff.terminal import TerminalProgress, format_header, format_outcome


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


def test_terminal_progress_prints_a_numbered_line_per_table(
    tmp_path: Path, write_two_datasets, capsys
):
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
    assert lines[1].split()[0] == "STATUS"
    assert re.match(r"\[1/2\]\s+\[IDENTICAL\].*table_a$", lines[2])
    assert re.match(r"\[2/2\]\s+\[IDENTICAL\].*table_b$", lines[3])
    assert [o.table_name for o in progress.outcomes] == ["table_a", "table_b"]
