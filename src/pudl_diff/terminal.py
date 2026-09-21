"""Text rendering of a PUDL diff for a terminal."""

from collections.abc import Callable
from pathlib import Path

import click

from pudl_diff import table_report
from pudl_diff.dataset_report import (
    PudlDiffReport,
    PudlDiffSummary,
    TableOutcome,
    table_outcome,
)
from pudl_diff.formatting import (
    format_duration,
    format_elapsed,
    format_percent,
    format_signed_percent,
)

_GRAY = "bright_black"
_TAGS = {
    0: ("[IDENTICAL]", "green"),
    1: ("[CHANGED]", "yellow"),
    2: ("[ERROR]", "red"),
}
"""The log-level style tag and its color for each :attr:`~.TableOutcome.exit_code`."""
_TAG_WIDTH = 11
_KEY_WIDTH = 5
_LEFT_COLUMNS_WIDTH = 4
_COLUMNS_WIDTH = 14
_LEFT_ROWS_WIDTH = 13
_ROWS_WIDTH = 24
_PERCENT_WIDTH = 22
_ELAPSED_WIDTH = 9
_SIZE_WIDTH = 9
_RIGHT_SIZE_WIDTH = 10
_SIZE_CHANGE_WIDTH = 11
_PERCENT_CHANGE_WIDTH = 9

_Color = str | int
"""A color name click knows, or an ANSI 256-color code."""
_HOT_PINK = 205
_COLUMN_COLORS = ("cyan", _HOT_PINK, "magenta")
"""Colors for columns added, changed (dtype) and removed."""
_Segments = list[tuple[str, _Color | None]]
"""Pieces of text and the color (if any) to show each one in."""


def _tables(n: int) -> str:
    return f"{n:,} table{'' if n == 1 else 's'}"


def _render(segments: _Segments, width: int = 0) -> str:
    """Style ``segments``, padding to ``width`` based on their unstyled length."""
    plain = "".join(text for text, _ in segments)
    styled = "".join(
        click.style(text, fg=color) if color else text for text, color in segments
    )
    return styled + " " * max(0, width - len(plain))


def _render_right(segments: _Segments, width: int) -> str:
    """Style ``segments``, right-aligned in ``width`` based on their unstyled length."""
    plain = "".join(text for text, _ in segments)
    return " " * max(0, width - len(plain)) + _render(segments)


def _count(text: str, value: int, color: _Color) -> tuple[str, _Color]:
    """A count in ``color``, or gray if it's zero."""
    return text, color if value else _GRAY


def _change_segments(
    added: int,
    changed: int | None,
    removed: int,
    text: Callable[[int], str],
    colors: tuple[_Color, _Color, _Color] = ("green", "yellow", "red"),
) -> _Segments:
    """Git diff style ``+added/changed/-removed``, shown as ``text(count)``.

    For rows, ``added`` are rows only in the right table (green), ``changed`` are
    rows whose primary key is in both tables but whose values changed (yellow,
    and only for tables with a primary key), and ``removed`` are rows only in
    the left table (red). Zero counts are gray.
    """
    added_color, changed_color, removed_color = colors
    separator = ("/", _GRAY)
    segments = [_count(f"+{text(added)}", added, added_color), separator]
    if changed is not None:
        segments += [_count(text(changed), changed, changed_color), separator]
    return [*segments, _count(f"-{text(removed)}", removed, removed_color)]


def _columns_segments(outcome: TableOutcome) -> _Segments:
    """Summary of column changes: ``+added/changed/-removed``.

    Here ``changed`` counts the columns whose dtype changed. Cyan, hot pink and
    magenta, to tell them apart from the row counts.
    """
    if outcome.columns_added is None or outcome.columns_removed is None:
        return []
    return _change_segments(
        outcome.columns_added,
        outcome.dtypes_changed,
        outcome.columns_removed,
        lambda n: f"{n:,}",
        colors=_COLUMN_COLORS,
    )


def _size_change_segments(
    sizes: table_report.SizeComparison,
) -> tuple[_Segments, _Segments]:
    """The size change and its percentage of the left size, colored by direction.

    Blank if either size is unknown.
    """
    if sizes.bytes_difference is None or sizes.bytes_difference_size is None:
        return [], []
    # Green for a table that grew and red for one that shrank, like the +/- of the
    # row and column counts.
    if sizes.bytes_difference > 0:
        color: _Color = "green"
    else:
        color = "red" if sizes.bytes_difference < 0 else _GRAY
    percent = sizes.bytes_difference_percent
    return (
        [(sizes.bytes_difference_size, color)],
        [(format_signed_percent(percent), color)] if percent is not None else [],
    )


def _row_cells(outcome: TableOutcome) -> tuple[str, str]:
    """The styled, padded row changes and row change percentage cells.

    If there are no row changes to show, the explanation spans both cells.
    """
    message_width = _ROWS_WIDTH + 2 + _PERCENT_WIDTH
    if outcome.exit_code == 2:
        return _render([("comparison failed", None)], message_width), ""
    rows = outcome.rows
    if rows.skipped_reason is not None:
        reason = rows.skipped_reason.replace("_", " ")
        return _render([(f"row diff skipped: {reason}", None)], message_width), ""
    if rows.added is None or rows.removed is None:
        return _render([("no row-level results", None)], message_width), ""
    counts = _change_segments(
        rows.added, rows.changed, rows.removed, lambda n: f"{n:,}"
    )
    percents = _change_segments(
        rows.added,
        rows.changed,
        rows.removed,
        lambda n: format_percent(n, outcome.left_rows),
    )
    return _render(counts, _ROWS_WIDTH), _render(percents, _PERCENT_WIDTH)


def _format_key(has_primary_key: bool | None) -> str:
    if has_primary_key is None:
        return ""
    return "PK" if has_primary_key else "no-PK"


def format_header(progress_width: int = 0) -> str:
    """The two lines of column headings for the lines made by :func:`format_outcome`.

    A heading may name its column on the first line and say what it holds on the
    second, so that it needn't be wider than the values below it. The second line is
    the one that sits directly above the values.
    """
    # Each column's two lines of heading, its width, and whether it's right-aligned.
    columns = [
        (("", "STATUS"), _TAG_WIDTH, False),
        (("", "KEY"), _KEY_WIDTH, False),
        (("LEFT", "COLS"), _LEFT_COLUMNS_WIDTH, True),
        (("COL CHANGES", "+add/~chg/-del"), _COLUMNS_WIDTH, False),
        (("LEFT", "ROWS"), _LEFT_ROWS_WIDTH, True),
        (("ROW CHANGES", "+add/~chg/-del"), _ROWS_WIDTH, False),
        (("% OF LEFT ROWS", "+add/~chg/-del"), _PERCENT_WIDTH, False),
        (("", "LEFT SIZE"), _SIZE_WIDTH, True),
        (("", "RIGHT SIZE"), _RIGHT_SIZE_WIDTH, True),
        (("", "SIZE CHANGE"), _SIZE_CHANGE_WIDTH, True),
        (("", "% SIZE"), _PERCENT_CHANGE_WIDTH, True),
        (("", "TIME"), _ELAPSED_WIDTH, True),
        (("", "TABLE"), 0, False),
    ]
    lines = []
    for line in (0, 1):
        parts: list[str] = [" " * progress_width]
        for headings, width, right_aligned in columns:
            heading = headings[line]
            parts.append(
                heading.rjust(width) if right_aligned else heading.ljust(width)
            )
        lines.append(click.style("  ".join(p for p in parts if p).rstrip(), bold=True))
    return "\n".join(lines)


def format_outcome(outcome: TableOutcome, progress: str = "") -> str:
    """One line summarizing a table's comparison.

    The table name goes last, so that the (variable length) names don't disturb
    the alignment of everything before it.
    """
    tag, color = _TAGS[outcome.exit_code]
    elapsed = (
        format_elapsed(outcome.elapsed_seconds)
        if outcome.elapsed_seconds is not None
        else ""
    )
    left_rows = f"{outcome.left_rows:,}" if outcome.left_rows is not None else ""
    left_columns = (
        f"{outcome.left_columns:,}" if outcome.left_columns is not None else ""
    )
    row_counts, row_percents = _row_cells(outcome)
    size_change, size_percent = _size_change_segments(outcome.sizes)
    parts = [
        progress,
        click.style(tag.ljust(_TAG_WIDTH), fg=color),
        _format_key(outcome.rows.has_primary_key).ljust(_KEY_WIDTH),
        left_columns.rjust(_LEFT_COLUMNS_WIDTH),
        _render(_columns_segments(outcome), _COLUMNS_WIDTH),
        left_rows.rjust(_LEFT_ROWS_WIDTH),
        row_counts,
        row_percents,
        (outcome.sizes.left_table_size or "").rjust(_SIZE_WIDTH),
        (outcome.sizes.right_table_size or "").rjust(_RIGHT_SIZE_WIDTH),
        _render_right(size_change, _SIZE_CHANGE_WIDTH),
        _render_right(size_percent, _PERCENT_CHANGE_WIDTH),
        elapsed.rjust(_ELAPSED_WIDTH),
        outcome.table_name,
    ]
    return "  ".join(part for part in parts if part)


_LABEL_WIDTH = 17
_RULE_WIDTH = 60
"""Width of the line under the summary's headline."""


def _field(label: str, value: str) -> str:
    """A summary line: a bold ``label``, then ``value`` aligned with the others."""
    padding = " " * max(2, _LABEL_WIDTH - len(label))
    return f"{click.style(label, bold=True)}{padding}{value}"


def _echo_totals(summary: PudlDiffSummary) -> None:
    """Print row and size totals and changes summed over all the compared tables."""
    click.echo(
        _field(
            "Total rows:",
            f"{summary.left_row_count:,} left, {summary.right_row_count:,} right "
            f"({_tables(summary.table_count)})",
        )
    )
    counts = _change_segments(
        summary.rows_added,
        summary.rows_changed,
        summary.rows_removed,
        lambda n: f"{n:,}",
    )
    click.echo(_field("Row changes:", _render(counts)))
    percents = _change_segments(
        summary.rows_added,
        summary.rows_changed,
        summary.rows_removed,
        lambda n: format_percent(n, summary.left_row_count),
    )
    click.echo(_field("% of left rows:", _render(percents)))
    # Rows in tables we couldn't count changes for still count towards the total
    # above, so say how many there are to make the percentages interpretable.
    if summary.no_row_diff_table_count:
        click.echo(
            _field(
                "Not compared:",
                f"{_tables(summary.no_row_diff_table_count)} "
                f"({summary.no_row_diff_left_row_count:,} left rows) "
                "had no row-level comparison",
            )
        )
    if summary.left_table_size and summary.right_table_size:
        click.echo(
            _field(
                "Total size:",
                f"{summary.left_table_size} left, {summary.right_table_size} right",
            )
        )
        change, percent = _size_change_segments(summary)
        click.echo(_field("Size change:", _render([*change, (" ", None), *percent])))


def _echo_schema_totals(summary: PudlDiffSummary, outcomes: list[TableOutcome]) -> None:
    """Print columns added, changed (dtype) and removed across all the tables.

    Followed by each table whose schema changed, and how its columns changed.
    """
    columns = _change_segments(
        summary.columns_added,
        summary.columns_changed,
        summary.columns_removed,
        lambda n: f"{n:,}",
        colors=_COLUMN_COLORS,
    )
    click.echo(_field("Column changes:", _render(columns)))
    click.echo(_field("Schema changes:", _tables(len(summary.schema_changed_tables))))
    # Schema changes can be disruptive to users, so name every table that has one,
    # with its own column changes, one per line.
    by_name = {o.table_name: o for o in outcomes}
    width = max((len(name) for name in summary.schema_changed_tables), default=0)
    for name in summary.schema_changed_tables:
        counts = _render(_columns_segments(by_name[name]))
        click.echo(f"  {name.ljust(width)}  {counts}")


def _echo_table_list(label: str, table_names: list[str]) -> None:
    """Print a bold heading and a count, then the tables one per line."""
    if table_names:
        click.echo(f"{click.style(label + ':', bold=True)} {len(table_names):,}")
        for table_name in table_names:
            click.echo(f"  {table_name}")


def echo_summary(
    report: PudlDiffReport,
    outcomes: list[TableOutcome],
    report_path: Path,
    *,
    saved: bool = True,
) -> None:
    """Print how the run went: table counts, what was compared, time and memory.

    Ends by saying the report was written to ``report_path``, or, if ``saved`` is
    False, that it was read from there.
    """
    summary = report.summary
    counts = [
        ("Identical", summary.identical_table_count, "green"),
        ("Changed", summary.changed_table_count, "yellow"),
        ("Error", summary.failed_table_count, "red"),
    ]
    click.echo(
        "\n"
        + "  ".join(
            f"{click.style(label, fg=color, bold=True)}: {click.style(str(n), bold=True)}"
            for label, n, color in counts
        )
    )
    click.echo("─" * _RULE_WIDTH)
    click.echo(_field("Left:", report.left_dataset.root))
    click.echo(_field("Right:", report.right_dataset.root))
    if report.elapsed_seconds is not None:
        click.echo(_field("Elapsed:", format_duration(report.elapsed_seconds)))
    if summary.peak_rss is not None:
        click.echo(
            _field("Peak memory:", f"{summary.peak_rss} ({summary.peak_rss_table})")
        )
    _echo_totals(summary)
    _echo_schema_totals(summary, outcomes)
    _echo_table_list("Tables with errors", summary.failed_tables)
    _echo_table_list("Tables removed (only in left)", report.tables_only_in_left)
    _echo_table_list("Tables added (only in right)", report.tables_only_in_right)
    verb = "written to" if saved else "read from"
    click.echo(f"{click.style(f'Report {verb}', bold=True)} {report_path}")


def echo_intro(
    tables: list[str], left_root: str, right_root: str, *, explicit: bool
) -> None:
    """Say which tables are about to be compared, and between what."""
    if not explicit:
        click.echo(
            f"Comparing {len(tables)} tables present in both {left_root!r} and "
            f"{right_root!r}."
        )
        return
    what = repr(tables[0]) if len(tables) == 1 else f"{len(tables)} tables"
    click.echo(f"Comparing {what} between {left_root!r} and {right_root!r}.")


class TerminalProgress:
    """Prints the column headings, then a line about each table as it's compared.

    Meant to be used as the callbacks of :func:`~.run_dataset_diff`.
    Keeps each table's :class:`~.TableOutcome`, in
    :attr:`outcomes`, for the summary at the end.
    """

    def __init__(
        self,
        left_root: str,
        right_root: str,
        *,
        explicit: bool,
        show_progress: bool,
        intro: str | None = None,
    ):
        """Set up to describe a comparison of two datasets.

        Args:
            left_root: Where the left dataset is, for the introduction.
            right_root: Where the right dataset is.
            explicit: Whether the tables to compare were named, rather than being
                all those in both datasets.
            show_progress: Whether to start each line with a ``[n/total]`` count.
            intro: What to say before the column headings instead of the usual
                description of the comparison, e.g. when showing a saved report.
        """
        self._intro = intro
        self._left_root = left_root
        self._right_root = right_root
        self._explicit = explicit
        self._show_progress = show_progress
        self._total = 0
        self.outcomes: list[TableOutcome] = []

    def tables_resolved(self, tables: list[str]) -> None:
        """Say what's about to be compared, and print the column headings."""
        self._total = len(tables)
        if self._intro is not None:
            click.echo(self._intro)
        else:
            echo_intro(
                tables, self._left_root, self._right_root, explicit=self._explicit
            )
        total = self._total
        click.echo(
            format_header(len(f"[{total}/{total}]") if self._show_progress else 0)
        )

    def table_compared(
        self, table_name: str, report: table_report.TableDiffReport
    ) -> None:
        """Print a line about a table that has just been compared."""
        outcome = table_outcome(table_name, report)
        self.outcomes.append(outcome)
        width = len(str(self._total))
        progress = (
            f"[{len(self.outcomes):>{width}}/{self._total}]"
            if self._show_progress
            else ""
        )
        click.echo(format_outcome(outcome, progress))
