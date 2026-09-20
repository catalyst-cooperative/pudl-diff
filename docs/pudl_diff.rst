===============================================================================
PUDL Diff
===============================================================================

``pudl_diff`` compares tables between two PUDL Parquet datasets -- one table, or
every table the two datasets have in common -- and reports whether they're
**functionally identical**: same columns, same dtypes, same row
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

The underlying comparison logic lives in the :mod:`pudl.validate.diff` subpackage,
which the CLI described here wraps. See its docstrings for the programmatic API
(start with :func:`pudl.validate.diff.runner.run_dataset_diff`) if you want to run
comparisons from a script or notebook rather than the CLI, or
:mod:`pudl.scripts.pudl_diff` for the CLI's own implementation. The pipeline tests
in ``tests/pipeline/validate/pudl_diff_test.py`` also double as runnable examples
of the underlying functions.

-----
Usage
-----

.. code-block:: console

   $ pudl_diff [TABLE_NAME ...] [OPTIONS]

By default, ``pudl_diff`` compares the most recent successful nightly build
(``s3://pudl.catalyst.coop/nightly/``, the "left"/reference dataset) against your
local build's outputs at ``$PUDL_OUTPUT/parquet`` (the "right" dataset), so the
diff reads as "what's changed locally since the last nightly build." Override
``--left``/``--right`` to compare any two dataset roots instead -- each one is
either a local directory or a remote URL (e.g. an S3 bucket) containing a full
PUDL ETL run's Parquet files and a ``datapackage.json`` descriptor.

You can give one or more table names. If you give none, ``pudl_diff`` compares
**every table that has a Parquet file in both datasets**. Either way, tables are
compared one at a time in a single process (so PUDL is only imported once), and
when comparing more than one, a summary is printed at the end. For all tables,
the summary also lists tables present in only one dataset, which aren't
compared. ``--right-table`` only makes sense for a single table, so it requires
exactly one table name; ``--partition-expr`` applies to every table given, so it
requires at least one.

``-l``, ``-r`` and ``-o`` are shorthand for ``--left``, ``--right`` and
``--output-path``. Output is colorized when stdout is a terminal; use
``--color`` or ``--no-color`` to override that, e.g. to keep the colors when
paging through ``less -R``.

Run ``pudl_diff --help`` for the full list of options and a few example
invocations.

Examples
--------

Compare a table between your local build and the last nightly build (the
default comparison):

.. code-block:: console

   $ pudl_diff out_eia__yearly_generators

Compare several specific tables:

.. code-block:: console

   $ pudl_diff out_eia__yearly_generators out_eia__yearly_plants

Compare every table present in both datasets, e.g. to see how a local build on
another branch differs from a copy of the nightly build, writing all the reports
to a ``diffs`` directory:

.. code-block:: console

   $ pudl_diff --left ~/nightly --right $PUDL_OUTPUT/parquet --output-path diffs

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

Show a report you already have (a ``pudl_diff_report.json``, or the directory
containing one) as the same table and summary a new comparison would print,
without comparing anything again. It exits with the code that comparison did,
and can't be combined with the options that control a comparison:

.. code-block:: console

   $ pudl_diff --from-report path/to/pudl_diff_report.json

-------------------------
Interpreting the results
-------------------------

``pudl_diff`` prints a one-line summary for each table, and writes a single JSON
report covering every table to ``<output-path>/pudl_diff_report.json``
(``--output-path`` defaults to the current working directory). For a table found
to differ, it also writes two Parquet side-output files:

* ``<table_name>_left_only.parquet`` -- rows found only in the left dataset (for
  a table with a primary key, this also includes the left-hand values of rows
  whose primary key matches but whose other data differs).
* ``<right_table_name>_right_only.parquet`` -- the same, for the right dataset.

They are written next to the report, and the report refers to them by paths relative to
the report file, so the output directory can be moved or copied, and everything in it
can still be found.

Under a two-line header naming each column, each table's line starts with a status tag
for the comparison as a whole (including its schema): ``[IDENTICAL]`` (green),
``[CHANGED]`` (yellow) or ``[ERROR]`` (red, the comparison itself failed). It's
followed by whether the table has a primary key (``PK`` or ``no-PK``), the number of
columns in the left table and how many were added, changed (had their dtype change)
and removed, the number of rows in the left table, a summary of the row-level changes
in the style of ``git diff`` (as counts and as percentages of the left table's rows),
the size of each table's Parquet file and how it changed, the time taken, and the
table name:

.. code-block:: text

                       LEFT  COL CHANGES              LEFT  ROW CHANGES               % OF LEFT ROWS
   STATUS       KEY    COLS  +add/~chg/-del           ROWS  +add/~chg/-del            +add/~chg/-del          LEFT SIZE  RIGHT SIZE  SIZE CHANGE     % SIZE       TIME  TABLE
   [IDENTICAL]  PK       25  +0/0/-0               408,860  +0/0/-0                   +0%/0%/-0%                31.4 MB     31.4 MB          0 B         0%     0.043s  core_eia861__yearly_sales
   [CHANGED]    PK       18  +0/0/-0                12,450  +50/221/-764              +0.40%/1.78%/-6.14%        1.6 MB      1.5 MB    -110.0 KB     -6.67%     0.512s  core_eia860__scd_utilities
   [CHANGED]    no-PK    41  +2/3/-1                61,320  +0/-0                     +0%/-0%                    5.2 MB      5.8 MB    +600.0 KB    +11.54%     1.204s  core_eia923__monthly_fuel
   [CHANGED]    PK       12  +0/0/-0         1,017,748,176  row diff skipped: too many rows                      9.4 GB      9.4 GB          0 B         0%     2.310s  core_epacems__hourly_emissions

* Left columns: the number of columns in the left table.
* Column changes: the number of columns only in the right table (``+``, cyan), the
  number of columns in both tables whose dtype changed (``~``, hot pink), and the
  number only in the left table (``-``, magenta). A table whose columns or
  dtypes changed is always ``[CHANGED]``.
* Rows, ``+50`` (green): rows only in the right table. For a table with a
  primary key, these are rows whose primary key is only in the right table.
* Rows, ``221`` (yellow): for a table with a primary key only, the number of
  rows whose primary key is in both tables but whose other values changed.
* Rows, ``-764`` (red): rows only in the left table, or primary keys only in the
  left table.
* Percentages: each of those counts as a percentage of the left table's rows.
  Very small ones are shown as ``<0.01%``.
* Sizes: the size of the Parquet file in each dataset, then the change in size and
  as a percentage of the left size. Like the row counts, it is green if the table grew
  (``+``) and red if it shrank (``-``), and gray if it didn't change.

Zero counts are shown in gray. For a table with no primary key, rows are
compared as a multiset, so a row that appears a different number of times counts
towards ``+`` or ``-``. If row-level comparison was skipped, the rows column
says so (e.g. ``row diff skipped: too many rows``) instead of showing counts.

After the last table, ``pudl_diff`` prints how many tables were identical, changed and
errored (in bold, above a horizontal line), the left and right datasets that were
compared, the total time taken, and the peak memory used (the highest of any single
table's, along with which table it was). It also totals the rows in all the tables, left
and right, and the rows added, changed and removed across all of them, in counts and as
percentages of the total left rows. Tables with no row-level comparison still count
towards the total left rows, so a separate line says how many tables and rows that
applies to.

The summary totals the schema changes in the same colors as the per-table
lines: the columns added, changed (dtype) and removed across all the tables, and
how many tables had any schema change at all. Since schema changes can be
disruptive to users, every table whose schema changed is then listed, one per
line, with its own column changes. Last, the tables that errored,
that were removed (present only in the left dataset) and that were added
(present only in the right dataset) are listed, one per line under a heading
with their count.

Log messages
------------

Only log messages of ``ERROR`` severity or higher are shown, so that they don't
interrupt the report. Skipped comparisons and errors are still recorded in the
JSON report. Use ``--loglevel`` (e.g. ``--loglevel WARNING``) to see
more.

Exit codes
----------

When comparing all tables, the exit code is the highest one of any table, and a
table that fails doesn't stop the others from being compared.

* ``0`` -- the tables are functionally identical.
* ``1`` -- the tables differ, or row-level comparison couldn't be completed
  (e.g. the table was too large -- see :ref:`pudl-diff-skipped-comparisons`
  below). Check the JSON report to see what kind of difference was found.
* ``2`` -- the comparison itself failed to run, e.g. ``TABLE_NAME`` doesn't
  exist in one of the datasets, or a dataset's ``datapackage.json`` couldn't be
  read. The failed table's ``error`` field in the JSON report has the exception
  message and traceback. If the run failed as a whole (e.g. the datasets have no
  tables in common), the report's top-level ``error`` field says why.

The JSON report
----------------

The report describes the comparison of the two datasets as a whole, with a
``tables`` entry for each table compared, keyed by its name in the left dataset.
Every field of the report, at every level, is described in the
:doc:`pudl_diff_report_schema`, which is generated from the code and is also available
as a `JSON Schema <https://json-schema.org>`__ for validating a report. Its top level
holds:

* ``schema_version`` -- the version of the report format.
* ``created`` and ``elapsed_seconds`` -- when the report was generated, and how
  long the comparison took.
* ``left_dataset`` and ``right_dataset`` -- each dataset's ``root`` path or URL
  and its own provenance (build ID, creation timestamp, git SHA and tags, read
  from its ``datapackage.json`` if present), so a saved report can be traced
  back to the builds it compared. The ``root`` of a dataset on the local filesystem
  (and the paths of its tables) is always an absolute path with any symlinks resolved,
  however it was given, so it doesn't depend on the directory ``pudl_diff`` was run
  from.
* ``options`` -- the settings the comparison was run with (tolerances, row
  limits and partitioning).
* ``tables_only_in_left`` and ``tables_only_in_right`` -- tables that weren't
  compared because they're in only one dataset.
* ``summary`` -- totals over all the tables: how many were identical, changed or
  failed, row and column changes, total size and the change in it, and peak
  memory use.
* ``is_identical``, ``success`` and ``error`` -- whether every table is
  identical, whether every comparison completed, and why the run as a whole
  failed, if it did. Failures of individual tables are recorded on their own
  entries.

Each table's entry also records the size in bytes of the table's Parquet file
on each side (``left_table_bytes`` and ``right_table_bytes``), and the change
(``bytes_difference``, and as a percentage of the left size in
``bytes_difference_percent``), each alongside a human-readable version, e.g.
``12.3 MB``. A table can change size, through compression for example, even if
its contents haven't. The rest of the entry has three main sections,
corresponding to the three kinds of comparison ``pudl_diff`` runs:

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
  difference of whole rows, counted as a multiset: a row that appears a
  different number of times in the two tables is a difference, and
  ``multiplicity_changed_row_count`` says how many distinct rows that applies
  to. Each entry has a ``status`` that says which it is: ``"compared"`` for a full
  report, or ``"skipped"`` for one that contains only a ``skipped_reason``:
  ``primary_key_available`` for ``non_pk_diff`` on a table with a primary key,
  or ``no_primary_key`` for ``pk_diff`` on one without. See
  :ref:`pudl-diff-skipped-comparisons` below for other reasons a comparison
  doesn't run.

.. _pudl-diff-skipped-comparisons:

When row-level comparison is skipped
-------------------------------------

Row-level comparison is the most expensive part of the comparison, and
``pudl_diff`` skips it (falling back to just the schema and row-count results)
in a few situations, recorded as the ``skipped_reason`` of whichever of
``row_diff.pk_diff`` or ``row_diff.non_pk_diff`` would otherwise have run:

* ``too_many_rows`` -- either table has more rows than ``--max-compare-rows``
  (default 100,000,000), a memory-safety cutoff. Row-level comparison uses the
  Polars streaming engine and writes its results to temporary Parquet files.
  Rows are matched using 64-bit hashes of their primary key (or, for tables
  without one, of the whole row), so the in-memory side of each join needs only
  a few bytes per row, but that still grows with table size, and a table whose
  float values all differ slightly needs much more. Comparing tables above this
  size risks exhausting memory on typical hardware.
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
