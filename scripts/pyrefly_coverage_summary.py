#! /usr/bin/env python
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "click>=8.5",
# ]
# ///
"""Render ``pyrefly coverage report``'s JSON as a human-readable table.

Reads the JSON report from a file, or from stdin, and prints one row per module --
typable symbol count, how many are untyped, and the resulting coverage percentage --
followed by the same metrics summed across the whole project. It only needs ``click``,
so it can be run without installing this package, e.g.::

    pyrefly coverage report src | uv run scripts/pyrefly_coverage_summary.py
"""

import json
from typing import Any, TextIO

import click


def _coverage(n_typable: int, n_untyped: int) -> float:
    """The percentage of typable symbols that are typed."""
    if n_typable == 0:
        return 100.0
    return 100.0 * (n_typable - n_untyped) / n_typable


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("report", type=click.File("r"), default="-")
def main(report: TextIO) -> None:
    """Print a table of the type coverage in a pyrefly coverage JSON REPORT.

    REPORT is the file to read, or - (the default) for stdin.
    """
    data: dict[str, Any] = json.load(report)

    rows = sorted(
        (
            module["name"],
            module["n_typable"],
            module["n_untyped"],
            _coverage(module["n_typable"], module["n_untyped"]),
        )
        for module in data["module_reports"]
    )

    name_width = max((len(name) for name, *_ in rows), default=len("Module"))
    header = (
        f"{'Module':<{name_width}}  {'Typable':>7}  {'Untyped':>7}  {'Coverage':>8}"
    )
    click.echo(header)
    click.echo("-" * len(header))
    for name, n_typable, n_untyped, coverage in rows:
        click.echo(
            f"{name:<{name_width}}  {n_typable:>7}  {n_untyped:>7}  {coverage:>7.1f}%"
        )

    summary = data["summary"]
    click.echo("-" * len(header))
    click.echo(
        f"{'TOTAL':<{name_width}}  {summary['n_typable']:>7}  "
        f"{summary['n_untyped']:>7}  {summary['coverage']:>7.1f}%"
    )


if __name__ == "__main__":
    main()
