"""Unit tests for pudl_diff.dataset."""

import contextlib
import os
import warnings
from pathlib import Path

import polars as pl
import pytest

from pudl_diff.dataset import PudlDiffDataset


@pytest.fixture
def dataset(
    tmp_path: Path, write_datapackage, pk_resource, no_pk_resource
) -> PudlDiffDataset:
    """A dataset with a table that has a primary key, and one that doesn't."""
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
    """Table names."""
    assert dataset.table_names() == ["table_with_pk", "table_without_pk"]


def test_get_resource_unknown_table(dataset: PudlDiffDataset):
    """Asking a datapackage for a table it doesn't list is an error that names the table."""
    with pytest.raises(ValueError, match="not found in datapackage"):
        dataset.get_resource("nonexistent_table")


def test_primary_key(dataset: PudlDiffDataset):
    """Primary key."""
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
    """If neither the datapackage nor any fallback has a table, it is treated as keyless.

    There is a warning for each: that the datapackage couldn't say, and then that nothing
    else could either. Comparing the table as one with no primary key is the safe default.
    """
    mocker.patch("pudl_diff.dataset.fallback_primary_key", return_value=None)

    assert PudlDiffDataset(tmp_path).primary_key("some_table") == []
    assert mock_loggers["dataset"].warning.call_count == 2


def test_field_names(dataset: PudlDiffDataset):
    """Field names."""
    assert dataset.field_names("table_with_pk") == ["x", "y"]


def test_table_path(dataset: PudlDiffDataset, tmp_path: Path):
    """Table path."""
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
    """Tables are found from the Parquet files that are there, and not the datapackage.

    A local build's datapackage can be missing, or out of date, so it isn't relied on to
    say which tables can be compared. Files that aren't Parquet are ignored.
    """
    dataset = make_dataset(
        tmp_path / "ds",
        [pk_resource("only_in_datapackage", ["x"])],
        {"b_table": pl.DataFrame({"x": [1]}), "a_table": pl.DataFrame({"x": [1]})},
    )
    (tmp_path / "ds" / "not_a_table.txt").write_text("ignored")
    assert dataset.parquet_table_names() == ["a_table", "b_table"]


def test_scan_table(dataset: PudlDiffDataset):
    """Scan table."""
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

    Regression test: passing a real `bool` through to `pl.scan_parquet`
    raises `ValueError: invalid value for 'anon': 'True' (expected str)`
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
    """A dataset can name its descriptor something other than `datapackage.json`."""
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
    """PUDL's published outputs name their descriptor `pudl_parquet_datapackage.json`.

    That is recognised from the bucket, however it's written: `s3://` or `gs://`, or in the
    path of an `https://` URL. Anywhere else, the descriptor is `datapackage.json`.
    """
    assert PudlDiffDataset(root).descriptor_name == expected


def test_explicit_descriptor_name_overrides_default():
    """Explicit descriptor name overrides default."""
    dataset = PudlDiffDataset(
        "s3://pudl.catalyst.coop/nightly", descriptor_name="datapackage.json"
    )
    assert dataset.descriptor_name == "datapackage.json"


def test_a_relative_local_root_becomes_an_absolute_path(tmp_path: Path):
    """A local dataset is identified by its absolute path, whatever it was given as.

    Otherwise a report's paths would depend on the directory that `pudl_diff` was run
    from. The `..` in the path is resolved too.
    """
    (tmp_path / "some" / "dir").mkdir(parents=True)

    with contextlib.chdir(tmp_path):
        dataset = PudlDiffDataset("some/../some/dir")
        table_path = dataset.table_path("a_table")

    assert dataset.root.path == str((tmp_path / "some" / "dir").resolve())
    assert Path(table_path.path).is_absolute()
    assert table_path.path.endswith("/some/dir/a_table.parquet")


def test_a_local_root_has_its_symlinks_resolved(tmp_path: Path):
    """A local root that is a symlink is recorded as the directory that it points to."""
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)

    dataset = PudlDiffDataset(tmp_path / "link")

    assert dataset.root.path == str(real.resolve())


def test_a_local_root_has_its_home_directory_expanded(tmp_path: Path, mocker):
    """A local root has its home directory expanded."""
    mocker.patch.dict(os.environ, {"HOME": str(tmp_path)})

    dataset = PudlDiffDataset("~/nightly")

    assert dataset.root.path == str((tmp_path / "nightly").resolve())


def test_a_file_url_root_is_resolved_too(tmp_path: Path):
    """A local root given as a `file://` URL is made absolute and resolved like any other."""
    dataset = PudlDiffDataset(f"file://{tmp_path}/a/../dataset")

    assert dataset.root.path == str((tmp_path / "dataset").resolve())


def test_a_remote_root_is_left_as_it_is():
    """Only local roots are resolved: a remote one isn't made into a path on this machine."""
    dataset = PudlDiffDataset("s3://some-bucket/some/dir")

    assert str(dataset.root) == "s3://some-bucket/some/dir"


def test_display_root_defaults_to_the_root(dataset: PudlDiffDataset, tmp_path: Path):
    """Display root defaults to the root."""
    assert dataset.display_root == str(tmp_path.resolve())
    assert dataset.display_table_path("t") == str(dataset.table_path("t"))


def test_display_root_only_changes_what_is_recorded(
    tmp_path: Path, write_datapackage, no_pk_resource
):
    """`display_root` changes the name that reports use for a dataset, and nothing that is read.

    That's for naming the durable location of data that is read from somewhere faster or
    closer, such as a local copy of a bucket. The tables are still read from the real root.
    """
    write_datapackage(tmp_path, [no_pk_resource("t")])
    pl.DataFrame({"x": [1]}).write_parquet(tmp_path / "t.parquet")
    dataset = PudlDiffDataset(tmp_path, display_root="s3://pudl.catalyst.coop/v1/")

    assert dataset.display_root == "s3://pudl.catalyst.coop/v1/"
    assert dataset.display_table_path("t") == "s3://pudl.catalyst.coop/v1/t.parquet"
    assert dataset.table_path("t") == tmp_path.resolve() / "t.parquet"
    assert dataset.scan_table("t").collect().height == 1


def test_geoarrow_wkb_columns_load_as_binary_without_a_warning(tmp_path: Path):
    """Regression test: Polars warned that it didn't know the extension type."""
    geometry = pl.Series(
        "geom", [b"\x01", b"\x02"], dtype=pl.Extension("geoarrow.wkb", pl.Binary)
    )
    frame = pl.DataFrame({"x": [1, 2], "geom": geometry})
    frame.write_parquet(tmp_path / "t.parquet")

    # Polars raises its warning from Rust, where an error filter would be swallowed, so
    # record the warnings rather than relying on the tests turning them into errors.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        schema = PudlDiffDataset(tmp_path).scan_table("t").collect_schema()

    assert not [str(w.message) for w in caught]
    assert schema["geom"] == pl.Binary
