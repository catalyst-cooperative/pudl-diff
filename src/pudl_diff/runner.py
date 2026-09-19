"""Comparing many tables between two datasets."""

from pathlib import Path

import click

from pudl.validate.diff import table_report
from pudl.validate.diff.dataset import PudlDiffDataset
from pudl.validate.diff.dataset_report import (
    _outcome,
    _TableOutcome,
)
from pudl.validate.diff.terminal import (
    _format_header,
    _format_outcome,
)


def _compare_tables(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    tables: list[str],
    output_path: Path,
    *,
    right_table: str | None,
    options: table_report.DiffOptions,
    show_progress: bool,
) -> tuple[dict[str, table_report.TableDiffReport], list[_TableOutcome]]:
    """Compare each table in turn, printing a line about each as it's done."""
    reports: dict[str, table_report.TableDiffReport] = {}
    outcomes: list[_TableOutcome] = []
    total = len(tables)
    width = len(str(total))
    click.echo(_format_header(len(f"[{total}/{total}]") if show_progress else 0))
    for i, table in enumerate(tables, start=1):
        reports[table] = table_report.report_table_diff(
            left,
            right,
            table,
            output_path,
            right_table_name=right_table,
            options=options,
        )
        outcomes.append(_outcome(table, reports[table]))
        progress = f"[{i:>{width}}/{total}]" if show_progress else ""
        click.echo(_format_outcome(outcomes[-1], progress))
    return reports, outcomes
