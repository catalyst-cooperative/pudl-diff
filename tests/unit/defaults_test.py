"""Unit tests for pudl_diff.defaults."""

import ast
import types
from pathlib import Path

import pytest
from upath import UPath

from pudl_diff import defaults


@pytest.fixture(autouse=True)
def clear_nightly_cache():
    """Forget the nightly build's descriptor around each test, since it is cached."""
    defaults._nightly_descriptor.cache_clear()
    yield
    defaults._nightly_descriptor.cache_clear()


@pytest.fixture
def without_pudl(mocker):
    """Pretend PUDL isn't installed."""
    return mocker.patch.object(defaults, "_import_pudl", return_value=None)


def test_import_pudl_of_a_missing_module_is_none():
    """Import PUDL of a missing module is `None`."""
    assert defaults._import_pudl("no_such_package_anywhere") is None


def test_nightly_root_without_pudl(without_pudl):
    """Without PUDL installed, the nightly build is in PUDL's public S3 bucket, read anonymously."""
    root = defaults.nightly_root()

    assert str(root) == "s3://pudl.catalyst.coop/nightly"
    assert root.storage_options["anon"] is True


def test_nightly_root_uses_pudls_when_installed(mocker):
    """If PUDL is installed, its own idea of where the nightly builds are is used."""
    pudl_root = UPath("s3://elsewhere/nightly/", anon=True)
    mocker.patch.object(
        defaults,
        "_import_pudl",
        return_value=types.SimpleNamespace(PUDL_NIGHTLY_BUILDS_BASE_PATH=pudl_root),
    )

    assert defaults.nightly_root() == pudl_root


def test_default_right_root_without_pudl_uses_the_environment(
    without_pudl, tmp_path: Path, mocker
):
    """Without PUDL installed, the default right dataset is `$PUDL_OUTPUT/parquet`."""
    mocker.patch.dict("os.environ", {"PUDL_OUTPUT": str(tmp_path)})

    assert defaults.default_right_root() == tmp_path / "parquet"


def test_default_right_root_without_pudl_or_environment_raises(without_pudl, mocker):
    """Default right root without PUDL or environment raises."""
    mocker.patch.dict("os.environ", clear=True)

    with pytest.raises(RuntimeError, match="PUDL_OUTPUT"):
        defaults.default_right_root()


def test_default_right_root_uses_pudls_paths_when_installed(mocker, tmp_path: Path):
    """With PUDL installed, its own paths say where the outputs are, and not the environment."""
    paths = mocker.MagicMock()
    paths.PudlPaths.return_value.parquet_path.return_value = tmp_path / "p"
    mocker.patch.object(defaults, "_import_pudl", return_value=paths)

    assert defaults.default_right_root() == tmp_path / "p"


def _fake_pudl_package(mocker, primary_keys: dict[str, list[str]]):
    def get_resource(name: str):
        if name not in primary_keys:
            raise ValueError(name)
        resource = mocker.MagicMock()
        resource.schema.primary_key = primary_keys[name]
        return resource

    classes = mocker.MagicMock()
    classes.PUDL_PACKAGE.get_resource.side_effect = get_resource
    mocker.patch.object(defaults, "_import_pudl", return_value=classes)


def test_pudl_package_primary_key(mocker):
    """A table's primary key is read from PUDL's metadata, and a table it doesn't know is `None`.

    A table that PUDL knows and says has no key has an empty list, which isn't the same
    as not knowing about the table.
    """
    _fake_pudl_package(mocker, {"t": ["a", "b"], "keyless": []})

    assert defaults._pudl_package_primary_key("t") == ["a", "b"]
    assert defaults._pudl_package_primary_key("keyless") == []
    assert defaults._pudl_package_primary_key("unknown") is None


def test_pudl_package_primary_key_without_pudl(without_pudl):
    """PUDL package primary key without PUDL."""
    assert defaults._pudl_package_primary_key("t") is None


def _nightly_descriptor(mocker, text: str | Exception):
    path = mocker.MagicMock()
    if isinstance(text, Exception):
        path.read_text.side_effect = text
    else:
        path.read_text.return_value = text
    root = mocker.MagicMock()
    root.__truediv__.return_value = path
    mocker.patch.object(defaults, "nightly_root", return_value=root)
    return root


def test_nightly_primary_key_is_fetched_once(mocker):
    """The nightly build's datapackage is fetched once, however many tables are asked about.

    A table with no primary key has an empty one, and a table that isn't in the datapackage
    has `None`. The descriptor has the name that PUDL's published outputs give it.
    """
    descriptor = (
        '{"resources": [{"name": "t", "schema": {"primaryKey": ["x"]}},'
        ' {"name": "keyless", "schema": {}}]}'
    )
    root = _nightly_descriptor(mocker, descriptor)

    assert defaults._nightly_primary_key("t") == ["x"]
    assert defaults._nightly_primary_key("keyless") == []
    assert defaults._nightly_primary_key("unknown") is None
    root.__truediv__.assert_called_once_with("pudl_parquet_datapackage.json")


@pytest.mark.parametrize("failure", [OSError("offline"), "not json"])
def test_nightly_primary_key_when_the_descriptor_cant_be_read(
    mock_loggers, mocker, failure
):
    """If the nightly datapackage can't be fetched, or isn't JSON, there is no answer.

    That is `None`, with a warning, and not an exception, so that a comparison of local
    tables can carry on without a network connection.
    """
    _nightly_descriptor(mocker, failure)

    assert defaults._nightly_primary_key("t") is None
    mock_loggers["defaults"].warning.assert_called_once()


def test_fallback_primary_key_prefers_pudl_then_the_nightly_build(mocker):
    """PUDL's metadata is tried first, and the nightly build's datapackage only if it has none.

    When PUDL knows the table, the nightly datapackage isn't even fetched.
    """
    nightly = mocker.patch.object(defaults, "_nightly_primary_key", return_value=["n"])
    _fake_pudl_package(mocker, {"in_pudl": ["p"]})

    assert defaults.fallback_primary_key("in_pudl") == ["p"]
    nightly.assert_not_called()
    assert defaults.fallback_primary_key("only_nightly") == ["n"]


def test_fallback_primary_key_keyless_pudl_table_is_not_looked_up_elsewhere(mocker):
    """A table that PUDL says has no primary key has none, and nothing else is asked.

    An empty key is an answer, and not a gap to fill from the nightly build.
    """
    nightly = mocker.patch.object(defaults, "_nightly_primary_key", return_value=["n"])
    _fake_pudl_package(mocker, {"keyless": []})

    assert defaults.fallback_primary_key("keyless") == []
    nightly.assert_not_called()


def test_fallback_primary_key_found_nowhere(without_pudl, mocker):
    """If no source has the table, its primary key isn't known, which is `None`."""
    mocker.patch.object(defaults, "_nightly_primary_key", return_value=None)

    assert defaults.fallback_primary_key("t") is None


def test_only_the_defaults_module_depends_on_the_rest_of_pudl():
    """Keep the tool separable from PUDL: PUDL is imported only in defaults.py."""
    package = Path(defaults.__file__).parent
    modules = list(package.glob("*.py"))
    offenders = []
    for module in modules:
        if module.name == "defaults.py":
            continue
        for node in ast.walk(ast.parse(module.read_text(encoding="utf-8"))):
            names = (
                [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else []
            )
            offenders += [
                f"{module.name}: {name}"
                for name in names
                if (name == "pudl" or name.startswith("pudl."))
                and not name.startswith("pudl_diff")
            ]

    assert offenders == []
