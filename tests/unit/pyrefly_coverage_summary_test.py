"""Unit tests for pudl_diff.pyrefly_coverage_summary."""

import json
from pathlib import Path

from click.testing import CliRunner

from pudl_diff.pyrefly_coverage_summary import _coverage, main

REPORT = {
    "module_reports": [
        {"name": "pkg.zeta", "n_typable": 4, "n_untyped": 1},
        {"name": "pkg.alpha", "n_typable": 0, "n_untyped": 0},
    ],
    "summary": {"n_typable": 4, "n_untyped": 1, "coverage": 75.0},
}


def test_coverage_is_the_percentage_typed_and_full_if_nothing_is_typable():
    assert _coverage(4, 1) == 75.0
    assert _coverage(0, 0) == 100.0


def test_main_prints_a_row_per_module_and_the_total_from_stdin():
    result = CliRunner().invoke(main, input=json.dumps(REPORT))

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].split() == ["Module", "Typable", "Untyped", "Coverage"]
    assert lines[2].split() == ["pkg.alpha", "0", "0", "100.0%"]  # sorted by name
    assert lines[3].split() == ["pkg.zeta", "4", "1", "75.0%"]
    assert lines[5].split() == ["TOTAL", "4", "1", "75.0%"]


def test_main_reads_a_file(tmp_path: Path):
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(REPORT))

    result = CliRunner().invoke(main, [str(report_path)])

    assert result.exit_code == 0, result.output
    assert "pkg.zeta" in result.output


def test_main_of_a_report_without_modules():
    empty = {"module_reports": [], "summary": REPORT["summary"]}

    result = CliRunner().invoke(main, input=json.dumps(empty))

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0].split() == [
        "Module",
        "Typable",
        "Untyped",
        "Coverage",
    ]
