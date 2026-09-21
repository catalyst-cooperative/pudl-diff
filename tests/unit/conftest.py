"""Fixtures for building the datasets that the diff tests compare."""

import importlib
import json
import logging
import pkgutil
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from pytest_mock import MockerFixture

import pudl_diff
from pudl_diff.dataset import PudlDiffDataset


def _modules_with_loggers() -> list[str]:
    """The modules of the package that log, found so that new ones are covered too."""
    names = []
    for module_info in pkgutil.iter_modules(pudl_diff.__path__):
        module = importlib.import_module(f"pudl_diff.{module_info.name}")
        if isinstance(getattr(module, "logger", None), logging.Logger):
            names.append(module_info.name)
    return names


@pytest.fixture(autouse=True)
def mock_loggers(mocker: MockerFixture) -> dict[str, MagicMock]:
    """Replace the logger of each module with a mock, so tests are quiet.

    Many tests exercise expected failures, which would otherwise log alarming errors
    and warnings into the output of any test that fails. A test that cares what was
    logged can ask for this fixture, and check the mock of a module's logger, which
    is keyed by the module's name, e.g. ``mock_loggers["table"]``.
    """
    return {
        name: mocker.patch(f"pudl_diff.{name}.logger", spec=True)
        for name in _modules_with_loggers()
    }


# Each fixture provides a function, so that a test can build as many datasets as it
# needs.


def _write_datapackage(
    root: Path, resources: list[dict], descriptor_name: str = "datapackage.json"
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / descriptor_name).write_text(
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
) -> PudlDiffDataset:
    _write_datapackage(root, resources)
    for table_name, df in tables.items():
        df.write_parquet(root / f"{table_name}.parquet")
    return PudlDiffDataset(root)


def _write_two_datasets(
    tmp_path: Path,
    left_tables: dict[str, list[str]],
    right_tables: dict[str, list[str]],
) -> tuple[PudlDiffDataset, PudlDiffDataset]:
    """Two datasets of tables with an ``x`` primary key and a ``y`` column."""
    datasets = []
    for name, tables in (("left", left_tables), ("right", right_tables)):
        root = tmp_path / name
        _write_datapackage(root, [_pk_resource(t, ["x"]) for t in tables])
        for table, ys in tables.items():
            pl.DataFrame({"x": list(range(len(ys))), "y": ys}).write_parquet(
                root / f"{table}.parquet"
            )
        datasets.append(PudlDiffDataset(root))
    return datasets[0], datasets[1]


@pytest.fixture(name="write_datapackage")
def write_datapackage_fixture() -> Callable:
    """The :func:`_write_datapackage` helper, as a fixture."""
    return _write_datapackage


@pytest.fixture(name="pk_resource")
def pk_resource_fixture() -> Callable:
    """The :func:`_pk_resource` helper, as a fixture."""
    return _pk_resource


@pytest.fixture(name="no_pk_resource")
def no_pk_resource_fixture() -> Callable:
    """The :func:`_no_pk_resource` helper, as a fixture."""
    return _no_pk_resource


@pytest.fixture(name="make_dataset")
def make_dataset_fixture() -> Callable:
    """The :func:`_make_dataset` helper, as a fixture."""
    return _make_dataset


@pytest.fixture(name="write_two_datasets")
def write_two_datasets_fixture() -> Callable:
    """The :func:`_write_two_datasets` helper, as a fixture."""
    return _write_two_datasets
