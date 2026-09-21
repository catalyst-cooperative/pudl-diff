"""Unit tests for pudl_diff.dataset."""

import contextlib
import os
from pathlib import Path

import polars as pl
import pytest

from pudl_diff.dataset import PudlDiffDataset


@pytest.fixture
def dataset(
    tmp_path: Path, write_datapackage, pk_resource, no_pk_resource
) -> PudlDiffDataset:
    write_datapackage(
        tmp_path,
        [pk_resource("table_with_pk", ["x"]), no_pk_resource("table_without_pk")],
    )
    pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}).write_parquet(
        tmp_path / "table_with_pk.parquet"
    )
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_parquet(
        tmp_path / "table_without_pk.parquet"
    )
    return PudlDiffDataset(tmp_path)


def test_table_names(dataset: PudlDiffDataset):
    assert dataset.table_names() == ["table_with_pk", "table_without_pk"]


def test_get_resource_unknown_table(dataset: PudlDiffDataset):
    with pytest.raises(ValueError, match="not found in datapackage"):
        dataset.get_resource("nonexistent_table")


def test_primary_key(dataset: PudlDiffDataset):
    assert dataset.primary_key("table_with_pk") == ["x"]
    assert dataset.primary_key("table_without_pk") == []


def test_primary_key_falls_back_when_the_table_is_missing_from_the_datapackage(
    mock_loggers, dataset: PudlDiffDataset, mocker
):
    """A table absent from the local datapackage uses the fallback metadata."""
    fallback = mocker.patch(
        "pudl_diff.dataset.fallback_primary_key", return_value=["code"]
    )

    assert dataset.primary_key("some_other_table") == ["code"]
    fallback.assert_called_once_with("some_other_table")
    mock_loggers["dataset"].warning.assert_called_once()


def test_primary_key_falls_back_when_the_datapackage_is_missing(
    mock_loggers, tmp_path: Path, mocker
):
    """A dataset with no datapackage.json at all also falls back cleanly."""
    mocker.patch("pudl_diff.dataset.fallback_primary_key", return_value=["code"])

    assert PudlDiffDataset(tmp_path).primary_key("some_table") == ["code"]
    mock_loggers["dataset"].warning.assert_called_once()


def test_primary_key_is_empty_if_no_fallback_finds_one(
    mock_loggers, tmp_path: Path, mocker
):
    mocker.patch("pudl_diff.dataset.fallback_primary_key", return_value=None)

    assert PudlDiffDataset(tmp_path).primary_key("some_table") == []
    assert mock_loggers["dataset"].warning.call_count == 2


def test_field_names(dataset: PudlDiffDataset):
    assert dataset.field_names("table_with_pk") == ["x", "y"]


def test_table_path(dataset: PudlDiffDataset, tmp_path: Path):
    assert dataset.table_path("table_with_pk") == tmp_path / "table_with_pk.parquet"


def test_table_path_unknown_table_is_still_deterministic(
    dataset: PudlDiffDataset, tmp_path: Path
):
    """table_path() doesn't validate against the datapackage - see scan_table()."""
    assert (
        dataset.table_path("nonexistent_table")
        == tmp_path / "nonexistent_table.parquet"
    )


def test_parquet_table_names_lists_files_not_datapackage_entries(
    tmp_path: Path, pk_resource, make_dataset
):
    dataset = make_dataset(
        tmp_path / "ds",
        [pk_resource("only_in_datapackage", ["x"])],
        {"b_table": pl.DataFrame({"x": [1]}), "a_table": pl.DataFrame({"x": [1]})},
    )
    (tmp_path / "ds" / "not_a_table.txt").write_text("ignored")
    assert dataset.parquet_table_names() == ["a_table", "b_table"]


def test_scan_table(dataset: PudlDiffDataset):
    df = dataset.scan_table("table_with_pk").collect()
    assert df.to_dict(as_series=False) == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_scan_table_unknown_table(dataset: PudlDiffDataset):
    """No file at the deterministic path raises Polars' own clear error."""
    with pytest.raises(FileNotFoundError):
        dataset.scan_table("nonexistent_table").collect_schema()


def test_scan_table_stringifies_non_string_storage_options(
    dataset: PudlDiffDataset, mocker
):
    """Polars' storage_options rejects non-string values, e.g. UPath's anon=True.

    Regression test: passing a real ``bool`` through to ``pl.scan_parquet``
    raises ``ValueError: invalid value for 'anon': 'True' (expected str)``
    deep inside its Rust backend, even though the UPath itself is happy to
    hand back non-string option values.
    """
    mocker.patch.object(
        type(dataset.root),
        "storage_options",
        new_callable=mocker.PropertyMock,
        return_value={"anon": True, "region": "us-west-2"},
    )
    scan_parquet_spy = mocker.spy(pl, "scan_parquet")
    dataset.scan_table("table_with_pk")
    _, kwargs = scan_parquet_spy.call_args
    assert kwargs["storage_options"] == {"anon": "True", "region": "us-west-2"}


def test_custom_descriptor_name(tmp_path: Path, write_datapackage, pk_resource):
    write_datapackage(
        tmp_path,
        [pk_resource("table_with_pk", ["x"])],
        descriptor_name="pudl_parquet_datapackage.json",
    )
    dataset = PudlDiffDataset(tmp_path, descriptor_name="pudl_parquet_datapackage.json")
    assert dataset.table_names() == ["table_with_pk"]
    assert dataset.primary_key("table_with_pk") == ["x"]


@pytest.mark.parametrize(
    ("root", "expected"),
    [
        ("/local/pudl/output/parquet", "datapackage.json"),
        ("s3://pudl.catalyst.coop/nightly", "pudl_parquet_datapackage.json"),
        ("gs://pudl.catalyst.coop/stable", "pudl_parquet_datapackage.json"),
        ("s3://some-other-bucket/nightly", "datapackage.json"),
        (
            "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly",
            "pudl_parquet_datapackage.json",
        ),
        (
            "https://s3.us-west-2.amazonaws.com/some-other-bucket/nightly",
            "datapackage.json",
        ),
    ],
)
def test_default_descriptor_name(root: str, expected: str):
    assert PudlDiffDataset(root).descriptor_name == expected


def test_explicit_descriptor_name_overrides_default():
    dataset = PudlDiffDataset(
        "s3://pudl.catalyst.coop/nightly", descriptor_name="datapackage.json"
    )
    assert dataset.descriptor_name == "datapackage.json"


def test_a_relative_local_root_becomes_an_absolute_path(tmp_path: Path):
    (tmp_path / "some" / "dir").mkdir(parents=True)

    with contextlib.chdir(tmp_path):
        dataset = PudlDiffDataset("some/../some/dir")
        table_path = dataset.table_path("a_table")

    assert dataset.root.path == str((tmp_path / "some" / "dir").resolve())
    assert Path(table_path.path).is_absolute()
    assert table_path.path.endswith("/some/dir/a_table.parquet")


def test_a_local_root_has_its_symlinks_resolved(tmp_path: Path):
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)

    dataset = PudlDiffDataset(tmp_path / "link")

    assert dataset.root.path == str(real.resolve())


def test_a_local_root_has_its_home_directory_expanded(tmp_path: Path, mocker):
    mocker.patch.dict(os.environ, {"HOME": str(tmp_path)})

    dataset = PudlDiffDataset("~/nightly")

    assert dataset.root.path == str((tmp_path / "nightly").resolve())


def test_a_file_url_root_is_resolved_too(tmp_path: Path):
    dataset = PudlDiffDataset(f"file://{tmp_path}/a/../dataset")

    assert dataset.root.path == str((tmp_path / "dataset").resolve())


def test_a_remote_root_is_left_as_it_is():
    dataset = PudlDiffDataset("s3://some-bucket/some/dir")

    assert str(dataset.root) == "s3://some-bucket/some/dir"


def test_display_root_defaults_to_the_root(dataset: PudlDiffDataset, tmp_path: Path):
    assert dataset.display_root == str(tmp_path.resolve())
    assert dataset.display_table_path("t") == str(dataset.table_path("t"))


def test_display_root_only_changes_what_is_recorded(
    tmp_path: Path, write_datapackage, no_pk_resource
):
    write_datapackage(tmp_path, [no_pk_resource("t")])
    pl.DataFrame({"x": [1]}).write_parquet(tmp_path / "t.parquet")
    dataset = PudlDiffDataset(tmp_path, display_root="s3://pudl.catalyst.coop/v1/")

    assert dataset.display_root == "s3://pudl.catalyst.coop/v1/"
    assert dataset.display_table_path("t") == "s3://pudl.catalyst.coop/v1/t.parquet"
    assert dataset.table_path("t") == tmp_path.resolve() / "t.parquet"
    assert dataset.scan_table("t").collect().height == 1
