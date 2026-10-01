"""Test the console scripts of the installed package from within PyTest."""

import importlib.metadata
import json
from pathlib import Path

import polars as pl
import pytest

# Obtain a list of all deployed entry point scripts to test:
ENTRY_POINTS = importlib.metadata.distribution("catalystcoop.pudl_diff").entry_points


@pytest.mark.parametrize("ep", ENTRY_POINTS)
@pytest.mark.script_launch_mode("inprocess")
def test_console_scripts_help(script_runner, ep: importlib.metadata.EntryPoint) -> None:
    """Run each deployed console script with --help as a basic test.

    The script_runner fixture is provided by the pytest-console-scripts plugin.
    """
    ret = script_runner.run([ep.name, "--help"], print_result=False)
    assert ret.success


def _write_dataset(root: Path, ys: list[str]) -> None:
    root.mkdir()
    field_names = {"x": "integer", "y": "string"}
    resource = {
        "name": "t",
        "schema": {
            "fields": [{"name": n, "type": t} for n, t in field_names.items()],
            "primaryKey": ["x"],
        },
    }
    (root / "datapackage.json").write_text(json.dumps({"resources": [resource]}))
    pl.DataFrame({"x": list(range(len(ys))), "y": ys}).write_parquet(root / "t.parquet")


@pytest.mark.script_launch_mode("inprocess")
def test_pudl_diff_compares_two_datasets_and_shows_the_report_again(
    script_runner, tmp_path: Path
) -> None:
    """The installed script compares datasets, and replays the report it wrote."""
    _write_dataset(tmp_path / "left", ["a", "b"])
    _write_dataset(tmp_path / "right", ["a", "c"])
    out = tmp_path / "out"
    args = ["-l", str(tmp_path / "left"), "-r", str(tmp_path / "right"), "-o", str(out)]

    ret = script_runner.run(["pudl_diff", *args])
    replay = script_runner.run(["pudl_diff", "--from-report", str(out)])

    assert ret.returncode == 1  # the datasets differ
    assert "⚠️" in ret.stdout
    assert replay.returncode == 1
    assert "⚠️" in replay.stdout


@pytest.mark.script_launch_mode("inprocess")
def test_pudl_diff_rejects_a_report_that_is_not_one(
    script_runner, tmp_path: Path
) -> None:
    """The installed script says so, on stderr, when `--from-report` isn't given a report."""
    (tmp_path / "junk.json").write_text("{}")

    ret = script_runner.run(["pudl_diff", "--from-report", str(tmp_path / "junk.json")])

    assert not ret.success
    assert "Couldn't read a report" in ret.stderr
