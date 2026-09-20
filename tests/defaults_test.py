"""Unit tests for pudl.validate.diff.defaults."""

import ast
import types
from pathlib import Path

import pytest
from upath import UPath

from pudl.validate.diff import defaults


@pytest.fixture(autouse=True)
def clear_nightly_cache():
    defaults._nightly_descriptor.cache_clear()
    yield
    defaults._nightly_descriptor.cache_clear()


@pytest.fixture
def without_pudl(mocker):
    """Pretend PUDL isn't installed."""
    return mocker.patch.object(defaults, "_import_pudl", return_value=None)


def test_import_pudl_of_a_missing_module_is_none():
    assert defaults._import_pudl("no_such_package_anywhere") is None


def test_nightly_root_without_pudl(without_pudl):
    root = defaults.nightly_root()

    assert str(root) == "s3://pudl.catalyst.coop/nightly"
    assert root.storage_options["anon"] is True


def test_nightly_root_uses_pudls_when_installed(mocker):
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
    mocker.patch.dict("os.environ", {"PUDL_OUTPUT": str(tmp_path)})

    assert defaults.default_right_root() == tmp_path / "parquet"


def test_default_right_root_without_pudl_or_environment_raises(without_pudl, mocker):
    mocker.patch.dict("os.environ", clear=True)

    with pytest.raises(RuntimeError, match="PUDL_OUTPUT"):
        defaults.default_right_root()


def test_default_right_root_uses_pudls_paths_when_installed(mocker, tmp_path: Path):
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
    _fake_pudl_package(mocker, {"t": ["a", "b"], "keyless": []})

    assert defaults._pudl_package_primary_key("t") == ["a", "b"]
    assert defaults._pudl_package_primary_key("keyless") == []
    assert defaults._pudl_package_primary_key("unknown") is None


def test_pudl_package_primary_key_without_pudl(without_pudl):
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
def test_nightly_primary_key_when_the_descriptor_cant_be_read(mocker, failure):
    _nightly_descriptor(mocker, failure)

    assert defaults._nightly_primary_key("t") is None


def test_fallback_primary_key_prefers_pudl_then_the_nightly_build(mocker):
    nightly = mocker.patch.object(defaults, "_nightly_primary_key", return_value=["n"])
    _fake_pudl_package(mocker, {"in_pudl": ["p"]})

    assert defaults.fallback_primary_key("in_pudl") == ["p"]
    nightly.assert_not_called()
    assert defaults.fallback_primary_key("only_nightly") == ["n"]


def test_fallback_primary_key_keyless_pudl_table_is_not_looked_up_elsewhere(mocker):
    nightly = mocker.patch.object(defaults, "_nightly_primary_key", return_value=["n"])
    _fake_pudl_package(mocker, {"keyless": []})

    assert defaults.fallback_primary_key("keyless") == []
    nightly.assert_not_called()


def test_fallback_primary_key_found_nowhere(without_pudl, mocker):
    mocker.patch.object(defaults, "_nightly_primary_key", return_value=None)

    assert defaults.fallback_primary_key("t") is None


def test_only_the_defaults_module_depends_on_the_rest_of_pudl():
    """Keep the tool separable from PUDL: PUDL is imported only in defaults.py."""
    package = Path(defaults.__file__).parent
    modules = [*package.glob("*.py"), package.parents[1] / "scripts/pudl_diff.py"]
    offenders = []
    for module in modules:
        if module.name == "defaults.py":
            continue
        for node in ast.walk(ast.parse(module.read_text())):
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
                and not name.startswith("pudl.validate.diff")
            ]

    assert offenders == []
