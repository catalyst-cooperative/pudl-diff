"""Access to a PUDL Parquet dataset: its datapackage, tables, and provenance."""

import json
import os
from collections.abc import Sequence

import polars as pl
from upath import UPath

from pudl.validate.diff.base import ReportModel
from pudl.validate.diff.defaults import (
    PUDL_CATALYST_COOP_DESCRIPTOR_NAME,
    PUDL_CATALYST_COOP_HOST,
    fallback_primary_key,
)
from pudl.validate.diff.logs import get_logger

logger = get_logger(__name__)


class DatasetProvenance(ReportModel):
    """A PUDL dataset's own provenance, as recorded in its ``datapackage.json``.

    All fields are ``None`` when the dataset's descriptor doesn't have them
    - e.g. an older build predating git provenance, or one where the git
    lookup itself failed at build time.
    """

    id: str | None = None
    """The dataset's build UUID."""
    created: str | None = None
    """UTC ISO-8601 timestamp of when this dataset was built - distinct from the
    report's own :attr:`~.PudlDiffReport.created`, which is when the *comparison*
    was run."""
    git_sha: str | None = None
    """The git commit SHA of the PUDL code that built the dataset."""
    git_tags: list[str] | None = None
    """The git tags on that commit, e.g. release versions like ``v2026.1.0``."""


class PudlDiffDataset:
    """A PUDL Parquet dataset located at a root path, described by a datapackage.

    Wraps a root directory (local or remote, e.g. an S3 bucket) containing one
    ``<table_name>.parquet`` file per table and a datapackage descriptor.
    """

    def __init__(
        self,
        root: str | os.PathLike[str] | UPath,
        descriptor_name: str | None = None,
        display_root: str | None = None,
    ):
        """Initialize with the dataset's root path.

        Args:
            root: Path to the directory containing the Parquet files and the
                datapackage descriptor. A local path is made absolute, with any
                symlinks resolved, so that it doesn't depend on the working
                directory. May be a local path or a remote path
                (e.g. ``s3://...``) understood by :class:`upath.UPath`. If
                credentials or other filesystem options (e.g. ``anon=True`` for
                a public S3 bucket) are required, construct a
                :class:`~upath.UPath` with those options and pass it in
                directly.
            descriptor_name: Filename of the datapackage descriptor within
                ``root``. If not given, defaults to ``datapackage.json``, except
                when ``root`` points at PUDL's deployed public outputs at
                ``pudl.catalyst.coop``, which instead name it
                ``pudl_parquet_datapackage.json``.
            display_root: The root to record in reports instead of ``root``, if
                the dataset is read from somewhere other than its durable
                location. E.g. a build's outputs read from local disk that will be
                deployed to a permanent URL, or a public dataset read through a
                faster mirror. Purely descriptive: all reads use ``root``.
        """
        dataset_root = UPath(root)
        if dataset_root.protocol in ("", "file", "local"):
            # A dataset on the local filesystem is always identified by its absolute
            # path, so that it doesn't depend on the directory this was run from.
            if dataset_root.protocol == "":
                dataset_root = dataset_root.expanduser()
            dataset_root = dataset_root.resolve()
        self.root: UPath = dataset_root
        self.descriptor_name = descriptor_name or self._default_descriptor_name(
            self.root
        )
        self.display_root: str = display_root or str(self.root)
        self._datapackage: dict | None = None

    @staticmethod
    def _default_descriptor_name(root: UPath) -> str:
        # s3:// and gs:// URLs put the bucket name in the drive, e.g.
        # UPath("s3://pudl.catalyst.coop/nightly").drive == "pudl.catalyst.coop".
        # Path-style HTTPS URLs instead put it as the first path segment, e.g.
        # "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly".
        is_pudl_catalyst_coop = root.drive == PUDL_CATALYST_COOP_HOST or (
            root.protocol == "https" and root.parts[1:2] == (PUDL_CATALYST_COOP_HOST,)
        )
        if is_pudl_catalyst_coop:
            return PUDL_CATALYST_COOP_DESCRIPTOR_NAME
        return "datapackage.json"

    @property
    def datapackage(self) -> dict:
        """The parsed datapackage descriptor for this dataset."""
        if self._datapackage is None:
            path = self.root / self.descriptor_name
            self._datapackage = json.loads(path.read_text())
        return self._datapackage

    @property
    def _resources_by_name(self) -> dict[str, dict]:
        return {
            resource["name"]: resource
            for resource in self.datapackage.get("resources", [])
        }

    def table_names(self) -> list[str]:
        """All table names described by this dataset's datapackage."""
        return sorted(self._resources_by_name)

    def parquet_table_names(self) -> list[str]:
        """Names of all tables with a Parquet file directly in :attr:`root`.

        Unlike :meth:`table_names`, this lists the files actually present, so it
        doesn't depend on the datapackage descriptor existing or being current
        (common for local development outputs).
        """
        return sorted(
            path.name.removesuffix(".parquet") for path in self.root.glob("*.parquet")
        )

    def get_resource(self, table_name: str) -> dict:
        """Return the datapackage resource descriptor for ``table_name``.

        Raises:
            ValueError: if no resource with that name exists in the datapackage.
        """
        try:
            return self._resources_by_name[table_name]
        except KeyError:
            raise ValueError(
                f"Table {table_name!r} not found in datapackage at {self.root}"
            ) from None

    def primary_key(self, table_name: str) -> list[str]:
        """The primary key columns of ``table_name``, or an empty list if none.

        Read from this dataset's own datapackage descriptor if it's present,
        readable, and lists ``table_name``. Otherwise falls back on other metadata
        (see :func:`~.fallback_primary_key`): PUDL's own if it's installed, or else
        the last nightly build's datapackage. This matters for local development
        outputs, which may lack a datapackage.json entirely, or have one that's
        stale relative to the Parquet files actually sitting alongside it
        (e.g. a `$PUDL_OUTPUT/parquet` assembled by materializing individual
        assets across branches and sessions, rather than a single full ETL
        run). This is a best-effort fallback, logged when it's used so it's not
        silent: if the local output is stale enough that this table's primary key
        has since changed, the fallback's definition may not exactly match the file.
        If no primary key can be found anywhere, the table is treated as having none.
        """
        try:
            schema = self.get_resource(table_name)["schema"]
            return list(schema.get("primaryKey", []))
        except ValueError, OSError, json.JSONDecodeError:
            logger.warning(
                f"Couldn't read {table_name!r}'s primary key from the datapackage "
                f"at {self.root}; falling back to other metadata for it."
            )
        primary_key = fallback_primary_key(table_name)
        if primary_key is None:
            logger.warning(
                f"Couldn't find a primary key for {table_name!r} anywhere; treating "
                "it as having none."
            )
            return []
        return primary_key

    def field_names(self, table_name: str) -> list[str]:
        """The column names of ``table_name``, in datapackage order."""
        schema = self.get_resource(table_name)["schema"]
        return [field["name"] for field in schema["fields"]]

    def table_path(self, table_name: str) -> UPath:
        """Path to the Parquet file backing ``table_name``.

        Deterministic from :attr:`root` and ``table_name`` alone - doesn't
        require the datapackage descriptor to exist or list this table, so
        that a missing or incomplete datapackage.json (common for local
        development outputs) doesn't block locating the file itself.
        :meth:`scan_table` will raise its own clear error if the file isn't
        actually there.
        """
        return self.root / f"{table_name}.parquet"

    def display_table_path(self, table_name: str) -> str:
        """The path to ``table_name``'s Parquet file, as recorded in reports.

        Like :meth:`table_path`, but under :attr:`display_root`.
        """
        if self.display_root == str(self.root):
            return str(self.table_path(table_name))
        return f"{self.display_root.rstrip('/')}/{table_name}.parquet"

    def table_bytes(self, table_name: str) -> int:
        """Size in bytes of the Parquet file backing ``table_name``.

        Works for both local and remote (e.g. S3) roots.
        """
        return self.table_path(table_name).stat().st_size

    def scan_table(self, table_name: str) -> pl.LazyFrame:
        """Lazily scan ``table_name`` as a Polars LazyFrame.

        Works for both local and remote (e.g. S3) roots. Remote reads use the
        storage options (e.g. credentials, ``anon``) configured on this
        dataset's :attr:`root` path.
        """
        path = self.table_path(table_name)
        # Polars' storage_options requires string values; UPath can hand back
        # non-string ones (e.g. anon=True as an actual bool), which raises a
        # ValueError deep inside polars' Rust backend rather than coercing them.
        storage_options = {
            key: str(value) for key, value in path.storage_options.items()
        } or None
        return pl.scan_parquet(str(path), storage_options=storage_options)

    def provenance(self) -> DatasetProvenance:
        """This dataset's own build provenance, from its datapackage descriptor.

        Fields the descriptor doesn't have are left ``None`` on the returned
        :class:`DatasetProvenance`, e.g. for a build predating git
        provenance tracking.
        """
        git_tags = self.datapackage.get("git_tags")
        return DatasetProvenance(
            id=self.datapackage.get("id"),
            created=self.datapackage.get("created"),
            git_sha=self.datapackage.get("git_sha"),
            git_tags=list(git_tags) if git_tags else None,
        )


class NoTablesError(Exception):
    """There are no tables to compare."""


def resolve_tables(
    left: PudlDiffDataset,
    right: PudlDiffDataset,
    table_names: Sequence[str],
) -> tuple[list[str], list[str], list[str]]:
    """Decide which tables to compare.

    Returns:
        The tables to compare: those given, or else every table with a Parquet
        file in both datasets. Then the tables found only in the left dataset and
        those found only in the right dataset, both empty when tables are given.

    Raises:
        NoTablesError: If no tables were given, and the datasets have none in
            common.
    """
    if table_names:
        return list(dict.fromkeys(table_names)), [], []
    left_tables = left.parquet_table_names()
    right_tables = right.parquet_table_names()
    tables = sorted(set(left_tables) & set(right_tables))
    if not tables:
        raise NoTablesError(
            f"No tables found in both {str(left.root)!r} ({len(left_tables)} tables) "
            f"and {str(right.root)!r} ({len(right_tables)} tables)."
        )
    return (
        tables,
        sorted(set(left_tables) - set(right_tables)),
        sorted(set(right_tables) - set(left_tables)),
    )
