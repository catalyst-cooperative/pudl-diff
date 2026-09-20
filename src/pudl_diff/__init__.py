"""Compare PUDL Parquet outputs between two dataset roots.

A "root" is a local or remote directory containing the Parquet outputs of a full
PUDL ETL run, along with a datapackage descriptor of those outputs (e.g.
``$PUDL_OUTPUT/parquet`` or ``s3://pudl.catalyst.coop/nightly``). This subpackage
lets callers load and compare tables between two such roots, and report on the
differences.

To compare two datasets, use :func:`~.run_dataset_diff`,
which returns a :class:`~.PudlDiffReport`. It is a
Pydantic model, so ``model_dump_json()`` and ``model_validate_json()`` write and read
the JSON report. The ``pudl_diff`` command line tool is a thin wrapper around it.

The modules are layered, each importing only from those before it in this list:

* :mod:`~pudl.validate.diff.base`, :mod:`~pudl.validate.diff.formatting`,
  :mod:`~pudl.validate.diff.dataset` and :mod:`~pudl.validate.diff.performance`: the
  base class of the report's models, formatting sizes and durations, access to a
  dataset and its tables, and sampling memory and CPU use.
* :mod:`~pudl.validate.diff.schema`, :mod:`~pudl.validate.diff.row_counts` and
  :mod:`~pudl.validate.diff.rows`: the three comparisons of a pair of tables, from
  cheapest to most expensive.
* :mod:`~pudl.validate.diff.table` and :mod:`~pudl.validate.diff.outputs`: running
  all of them on one table, and writing the differing rows to Parquet files.
* :mod:`~pudl.validate.diff.table_report` and
  :mod:`~pudl.validate.diff.dataset_report`: the serializable reports on one table
  and on a whole dataset.
* :mod:`~pudl.validate.diff.terminal`: rendering reports as text for a terminal.
* :mod:`~pudl.validate.diff.runner`: comparing many tables and building the report.
"""
