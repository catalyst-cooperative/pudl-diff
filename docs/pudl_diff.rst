===============================================================================
PUDL Diff
===============================================================================

``pudl_diff`` compares a single table between two PUDL Parquet datasets and reports
whether they're **functionally identical**: same columns, same dtypes, same row
count, and the same row contents (floating point columns are compared with
tolerance, like :func:`numpy.isclose`). It's useful whenever you need to confirm
that a change to the PUDL codebase, its dependencies, or the raw input data did --
or didn't -- change the data outputs, without eyeballing tables by hand.

Typical uses include:

* Confirming that a refactor (e.g. rewriting an asset from pandas to Polars, or
  updating a major dependency) didn't change its output data.
* Understanding exactly what changed in a table after a real data update, beyond
  what a row-count or schema check alone would tell you.
* Giving a coding agent a concrete, checkable definition of "no unintended data
  changes" to work against while it does a refactor or dependency migration.

The underlying comparison logic lives in :mod:`pudl.validate.diff`, which the CLI
described here wraps. See that module's docstrings for the programmatic API if
you want to run comparisons from a script or notebook rather than the CLI, or
:mod:`pudl.scripts.pudl_diff` for the CLI's own implementation. The pipeline tests
in ``tests/pipeline/validate/pudl_diff_test.py`` also double as runnable examples
of the underlying functions.

-----
Usage
-----

.. code-block:: console

   $ pudl_diff TABLE_NAME [OPTIONS]

By default, ``pudl_diff`` compares the most recent successful nightly build
(``s3://pudl.catalyst.coop/nightly/``, the "left"/reference dataset) against your
local build's outputs at ``$PUDL_OUTPUT/parquet`` (the "right" dataset), so the
diff reads as "what's changed locally since the last nightly build." Override
``--left``/``--right`` to compare any two dataset roots instead -- each one is
either a local directory or a remote URL (e.g. an S3 bucket) containing a full
PUDL ETL run's Parquet files and a ``datapackage.json`` descriptor.

Run ``pudl_diff --help`` for the full list of options and a few example
invocations.

Examples
--------

Compare a table between your local build and the last nightly build (the
default comparison):

.. code-block:: console

   $ pudl_diff out_eia__yearly_generators

Compare the same table between two arbitrary datasets, local or remote:

.. code-block:: console

   $ pudl_diff out_eia__yearly_generators \
       --left s3://pudl.catalyst.coop/stable --right $PUDL_OUTPUT/parquet

Compare a ``core_*`` table against the ``out_*`` table built from it, within
the same dataset -- useful for confirming an ``out_`` table only adds columns
on top of its ``core_`` table and doesn't otherwise change its data:

.. code-block:: console

   $ pudl_diff core_eia860__scd_utilities --right-table out_eia__yearly_utilities \
       --left $PUDL_OUTPUT/parquet

-------------------------
Interpreting the results
-------------------------

``pudl_diff`` prints a one-line summary and writes a JSON report to
``<output-path>/<table_name>_diff.json`` (``--output-path`` defaults to the
current working directory). For a table found to differ, it also writes two
Parquet side-output files:

* ``<table_name>_left_only.parquet`` -- rows found only in the left dataset (for
  a table with a primary key, this also includes the left-hand values of rows
  whose primary key matches but whose other data differs).
* ``<right_table_name>_right_only.parquet`` -- the same, for the right dataset.

Exit codes
----------

* ``0`` -- the tables are functionally identical.
* ``1`` -- the tables differ, or row-level comparison couldn't be completed
  (e.g. the table was too large -- see :ref:`pudl-diff-skipped-comparisons`
  below). Check the JSON report to see what kind of difference was found.
* ``2`` -- the comparison itself failed to run, e.g. ``TABLE_NAME`` doesn't
  exist in one of the datasets, or a dataset's ``datapackage.json`` couldn't be
  read. The JSON report's ``error`` field has the exception message and
  traceback.

The JSON report
----------------

The report has three main sections, corresponding to the three kinds of
comparison ``pudl_diff`` runs:

* ``schema_diff`` -- columns only in the left table, columns only in the right
  table, and any dtype changes for columns present in both. Column order
  doesn't matter.
* ``row_count_diff`` -- total row counts for each side, the change in row count
  from the left (reference) table to the right one (``row_count_difference``,
  positive if the right table has more rows), and (if the table has a
  `dbt row-count-per-partition test
  <https://docs.getdbt.com/best-practices/writing-custom-generic-tests>`__
  configured, e.g. by report year) a per-partition breakdown of any changes.
  Use ``--partition-expr`` to specify a different partition column or expression, or
  ``--no-auto-partition`` to always compare whole-table row counts.
* ``row_diff`` -- the row-level comparison, with a ``pk_diff`` and a
  ``non_pk_diff`` entry, only one of which is a full report. For a table with a
  primary key (looked up from the dataset's own ``datapackage.json``, or falling
  back to PUDL's own metadata if that's missing or stale), ``pk_diff`` reports
  whether the two tables share the same set of primary keys, and, for shared
  keys, a per-column count of how many rows have changed non-primary-key
  values. For a table without one, ``non_pk_diff`` reports the symmetric
  difference of whole rows. The other entry contains only a ``skipped_reason``:
  ``primary_key_available`` for ``non_pk_diff`` on a table with a primary key,
  or ``no_primary_key`` for ``pk_diff`` on one without. See
  :ref:`pudl-diff-skipped-comparisons` below for other reasons a comparison
  doesn't run.

Each dataset's own provenance (build ID, creation timestamp, git SHA and tags,
read from its ``datapackage.json`` if present) is also included, so a saved
report can be traced back to the builds it compared.

.. _pudl-diff-skipped-comparisons:

When row-level comparison is skipped
-------------------------------------

Row-level comparison is the most expensive part of the comparison, and
``pudl_diff`` skips it (falling back to just the schema and row-count results)
in a few situations, recorded as the ``skipped_reason`` of whichever of
``row_diff.pk_diff`` or ``row_diff.non_pk_diff`` would otherwise have run:

* ``too_many_rows`` -- either table has more rows than ``--max-compare-rows``
  (default 100,000,000), a memory-safety cutoff. Row-level comparison uses the
  Polars streaming engine and writes its results to temporary Parquet files, but
  its hash joins still hold one side's join keys in memory, so comparing tables
  above this size risks exhausting memory on typical hardware.
* ``incompatible_dtypes`` -- the join underlying the row-level comparison
  failed, most likely because a shared column has incompatible dtypes between
  the two tables.
* ``mismatched_columns`` -- the two tables don't have the same columns, and
  either there's no primary key to key the comparison on, or the primary key
  columns themselves aren't present in both tables.

A table whose row-level comparison was skipped is conservatively reported as
**not** identical (exit code ``1``), even if its schema and row counts match,
since row content hasn't actually been verified.

-------------------------
Comparing dbt row counts
-------------------------

``pudl_diff``'s row-count comparison reimplements the same logic as dbt's
``check_row_counts_per_partition`` test (see
:doc:`data_validation_reference`), so a partitioned row-count change reported
by ``pudl_diff`` should match what that dbt test would report for the same two
datasets. Unlike the dbt test, ``pudl_diff`` doesn't require a dedicated seed of
expected row counts -- it compares two live datasets directly.
