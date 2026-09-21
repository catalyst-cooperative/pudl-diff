"""Compare PUDL Parquet outputs between two dataset roots.

A "root" is a local or remote directory containing the Parquet outputs of a full
PUDL ETL run, along with a datapackage descriptor of those outputs (e.g.
`$PUDL_OUTPUT/parquet` or `s3://pudl.catalyst.coop/nightly`). This package
lets callers load and compare tables between two such roots, and report on the
differences.

To compare two datasets, use [`run_dataset_diff()`][pudl_diff.runner.run_dataset_diff],
which returns a [`PudlDiffReport`][pudl_diff.dataset_report.PudlDiffReport].
It is a Pydantic model, so `model_dump_json()` and `model_validate_json()` write and
read the JSON report.
The `pudl_diff` command line tool is a thin wrapper around it.

The modules are layered, each importing only from those before it in this list:

* [`logs`][pudl_diff.logs], [`datapackage`][pudl_diff.datapackage] and
  [`defaults`][pudl_diff.defaults]:
  standard library logging, the parts of a datapackage descriptor that the tool reads,
  and the few things the tool knows about PUDL (its nightly build, and where its
  metadata is), which are all optional so that the tool doesn't depend on the rest of
  PUDL.
* [`base`][pudl_diff.base], [`formatting`][pudl_diff.formatting],
  [`dataset`][pudl_diff.dataset] and [`performance`][pudl_diff.performance]:
  the base class of the report's models, formatting sizes and durations, access to a
  dataset and its tables, and sampling memory and CPU use.
* [`schema`][pudl_diff.schema], [`row_counts`][pudl_diff.row_counts] and
  [`rows`][pudl_diff.rows]:
  the three comparisons of a pair of tables, from cheapest to most expensive.
* [`table`][pudl_diff.table] and [`outputs`][pudl_diff.outputs]:
  running all of them on one table, and writing the differing rows to Parquet files.
* [`table_report`][pudl_diff.table_report] and
  [`dataset_report`][pudl_diff.dataset_report]:
  the serializable reports on one table and on a whole dataset.
* [`terminal`][pudl_diff.terminal]: rendering reports as text for a terminal.
* [`runner`][pudl_diff.runner]: comparing many tables and building the report.
"""

import importlib.metadata

import polars as pl

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__version__: str = importlib.metadata.version("catalystcoop.pudl_diff")
__projecturl__ = "https://github.com/catalyst-cooperative/pudl-diff"

# PUDL stores geometries as GeoArrow WKB, an Arrow extension type that Polars doesn't
# know, so it warns every time it loads one, and will load it as an extension type by
# default in Polars 2.0. Registering it as its storage type, binary, says that this is
# what we want: geometries are compared as the bytes they are stored as.
pl.register_extension_type("geoarrow.wkb", as_storage=True)
