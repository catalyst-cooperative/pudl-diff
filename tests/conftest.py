"""Fixtures for building datasets to compare in the diff tests.

Each fixture provides a function, so that a test can build as many datasets as it needs.
"""

import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from pudl.validate.diff.dataset import PudlDiffDataset


def _write_datapackage(
    root: Path, resources: list[dict], descriptor_name: str = "datapackage.json"
) -> None:
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
    root.mkdir()
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
        root.mkdir()
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
