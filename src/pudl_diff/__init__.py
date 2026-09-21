"""Compare PUDL Parquet outputs between two dataset roots.

A "root" is a local or remote directory containing the Parquet outputs of a full
PUDL ETL run, along with a datapackage descriptor of those outputs (e.g.
``$PUDL_OUTPUT/parquet`` or ``s3://pudl.catalyst.coop/nightly``). This package
lets callers load and compare tables between two such roots, and report on the
differences.

To compare two datasets, use :func:`~.run_dataset_diff`,
which returns a :class:`~.PudlDiffReport`. It is a
Pydantic model, so ``model_dump_json()`` and ``model_validate_json()`` write and read
the JSON report. The ``pudl_diff`` command line tool is a thin wrapper around it.

The modules are layered, each importing only from those before it in this list:

* :mod:`~pudl_diff.logs` and :mod:`~pudl_diff.defaults`: standard
  library logging, and the few things the tool knows about PUDL (its nightly build,
  and where its metadata is), which are all optional so that the tool doesn't depend
  on the rest of PUDL.
* :mod:`~pudl_diff.base`, :mod:`~pudl_diff.formatting`,
  :mod:`~pudl_diff.dataset` and :mod:`~pudl_diff.performance`: the
  base class of the report's models, formatting sizes and durations, access to a
  dataset and its tables, and sampling memory and CPU use.
* :mod:`~pudl_diff.schema`, :mod:`~pudl_diff.row_counts` and
  :mod:`~pudl_diff.rows`: the three comparisons of a pair of tables, from
  cheapest to most expensive.
* :mod:`~pudl_diff.table` and :mod:`~pudl_diff.outputs`: running
  all of them on one table, and writing the differing rows to Parquet files.
* :mod:`~pudl_diff.table_report` and
  :mod:`~pudl_diff.dataset_report`: the serializable reports on one table
  and on a whole dataset.
* :mod:`~pudl_diff.terminal`: rendering reports as text for a terminal.
* :mod:`~pudl_diff.runner`: comparing many tables and building the report.
"""

import importlib.metadata

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__version__: str = importlib.metadata.version("catalystcoop.pudl_diff")
__projecturl__ = "https://github.com/catalyst-cooperative/pudl-diff"
