"""What the tool knows about PUDL, without depending on it.

Everything here works when the PUDL package isn't installed, by falling back on
constants and on PUDL's published outputs. When PUDL *is* importable, as it is when
this runs inside a PUDL environment, its own definitions are used instead, so that
they can't drift from these fallbacks. PUDL is only imported when it's needed, never
when this module is, so PUDL can itself depend on this tool.
"""

import functools
import importlib
import json
import os
import types
from pathlib import Path

from upath import UPath

from pudl_diff.logs import get_logger

logger = get_logger(__name__)

PUDL_CATALYST_COOP_HOST = "pudl.catalyst.coop"
"""The bucket of PUDL's published outputs, on both S3 and GCS."""
PUDL_CATALYST_COOP_DESCRIPTOR_NAME = "pudl_parquet_datapackage.json"
"""What the published outputs at :data:`PUDL_CATALYST_COOP_HOST` call their
datapackage descriptor, instead of ``datapackage.json``."""
NIGHTLY_ROOT = f"s3://{PUDL_CATALYST_COOP_HOST}/nightly/"
"""The last nightly build's outputs, the reference point most diffs are measured
against."""


def _import_pudl(module: str = "pudl") -> types.ModuleType | None:
    """Import a module of the PUDL package, or ``None`` if it isn't installed."""
    try:
        return importlib.import_module(module)
    except ImportError:
        return None


def nightly_root() -> UPath:
    """Where the last nightly build's outputs are, readable anonymously."""
    if (pudl := _import_pudl()) is not None:
        return pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH
    return UPath(NIGHTLY_ROOT, anon=True)


def default_right_root() -> Path:
    """The Parquet outputs of the local PUDL build: ``$PUDL_OUTPUT/parquet``.

    Raises:
        RuntimeError: If PUDL's output directory can't be determined.
    """
    if (paths := _import_pudl("pudl.workspace.setup")) is not None:
        return paths.PudlPaths().parquet_path()
    if not (pudl_output := os.environ.get("PUDL_OUTPUT")):
        raise RuntimeError(
            "There's no default for the right dataset unless PUDL_OUTPUT is set, "
            "so give its root explicitly."
        )
    return Path(pudl_output) / "parquet"


def _pudl_package_primary_key(table_name: str) -> list[str] | None:
    """A table's primary key from PUDL's own metadata, if PUDL is installed."""
    if (classes := _import_pudl("pudl.metadata.classes")) is None:
        return None
    try:
        return list(classes.PUDL_PACKAGE.get_resource(table_name).schema.primary_key)
    except ValueError:
        return None


@functools.cache
def _nightly_descriptor() -> dict | None:
    """The datapackage descriptor of the last nightly build, fetched once.

    ``None`` if it can't be fetched, which is logged.
    """
    path = nightly_root() / PUDL_CATALYST_COOP_DESCRIPTOR_NAME
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as e:
        logger.warning(f"Couldn't read the nightly datapackage at {path}: {e!r}")
        return None


def _nightly_primary_key(table_name: str) -> list[str] | None:
    """A table's primary key from the last nightly build's datapackage."""
    if (descriptor := _nightly_descriptor()) is None:
        return None
    for resource in descriptor.get("resources", []):
        if resource.get("name") == table_name:
            return list(resource.get("schema", {}).get("primaryKey", []))
    return None


def fallback_primary_key(table_name: str) -> list[str] | None:
    """A table's primary key, when its own dataset can't say.

    Tries PUDL's metadata if PUDL is installed, and then the datapackage of the last
    nightly build. The latter is only fetched if needed, and only once.

    Returns:
        The primary key, an empty list if the table has none, or ``None`` if it
        couldn't be found anywhere.
    """
    for source in (_pudl_package_primary_key, _nightly_primary_key):
        if (primary_key := source(table_name)) is not None:
            return primary_key
    return None
