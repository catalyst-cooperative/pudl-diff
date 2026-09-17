# PUDL Diff: A tool for comparing PUDL outputs

## Motivation

It's often useful to be able to compare local PUDL outputs against a reference output to
understand how the data has changed as a result of changes to the PUDL codebase, raw
input data, or our software dependencies.

Historically we have done these comparisons in an ad-hoc way, but it would be better to
have shared tooling that covers basic use cases, is well documented, easy to use, gets
tested, can be run reproducibly, and can be extended incrementally over time to
accumulate new value and functionality for everyone.

## Proposed Solution

We will consolidate all of our personal PUDL data diffing functionality into a shared,
re-usable resource that’s part of the core PUDL codebase, and is well tested and
documented. It will live in a new module `src/pudl/validate/diff.py`

Each PUDL dataset to be compared will be defined by a root path, which can be either a
local path or a remote path (e.g. an S3 bucket). The root path will contain all of the
Parquet outputs for a full PUDL ETL run, as well as a `datapackage.json` descriptor. By
default we will compare the local PUDL dataset at `$PUDL_OUTPUT/parquet` against the
outputs of the most recent successful nightly build at
`s3://pudl.catalyst.coop/nightly/`. However, users will be able to override these
defaults and provide any two root paths to compare. In addition to the root path, most
comparisons will need to know which table to compare, and potentially also which
specific data column or columns to compare.

For the purposes of this tool, we will consider tables "functionally identical" if:

- They have the same set of columns (order does not matter).
- The columns in the two tables share the same dtypes.
- The tables have the same number of rows.
- The rows contents are functionally identical (order does not matter).
- Floating point columns are considered functionally identical if they are "close
  enough" to each other, as determined by the `numpy.isclose()` function or equivalent
  Polars / DuckDB functionality.

## Core Functionality

This functionality will be implemented in the first iteration of the tool. It will live
in `src/pudl/validate/diff.py`. It will be used by the CLI script
`src/pudl/scripts/pudl_diff.py` and by a Marimo notebook for interactive exploration and
visualization of the differences between two PUDL datasets. This functionality will need
to have robust unit tests defined in `tests/unit/validate/test_diff.py`.

Questions the tool should allow users to answer include:

### At the table level:

* Are the two instances of the same table functionally identical?
* If the column dtypes are not identical, how do they differ?
* If the number of rows is not identical, how do they differ, and within which
  partitions of the table are the differences to be found? This question is equivalent
  to our `check_row_counts_by_partition` dbt data test.

### For Tables with Primary Keys

* We will use the PUDL schema contained in the `datapackage.json` descriptor to
  determine which tables have primary keys, and what those primary keys are.
* Do the two instances of the same table share the same set of primary keys?
  * If not, what is the symmetric difference of their primary keys?
  * If so, are the contents of the non-PK columns functionally identical?
* If the two instances of the same table have **different** primary keys:
  * What is the [**symmetric
    difference**](https://en.wikipedia.org/wiki/Symmetric_difference) of their primary
    keys?
  * What do the rows that are part of the symmetric difference contain?
  * We must be able to return these rows as a pandas or polars dataframe
  * We must be able to label the rows according to which dataset they came from
  * We must be able return the rows from only the left, or only the right dataset, that
    do not have a matching primary key in the other dataset.
  * We must be able to return all rows associated with primary keys that do not appear
    in both datasets.
* For tables in which the primary keys are **identical**
  * Are the contents of the non-PK columns functionally identical?
  * If not, we must be able to summarize how a given data column is different.
    * For numerical columns, we will need to generate an X-Y scatterplot.
    * For categorical columns, we will need to generate an X-Y heatmap (where each axis
      is the set of unique values in the column, and the color of each cell is the
      number of rows that have that combination of values).
  * If not, we must be able to show a sample (or all) of the rows that aren’t identical.

### For Tables without Primary Keys

* Are the two instances of the same table functionally identical?
* If not, we must be able to select and return only the rows that are part of the
  symmetric difference, with a label indicating which dataset they came from.
* We must be able to return these rows as a pandas or polars dataframe.
* We must be able to label the rows according to which dataset they came from.
* We must be able return the rows from only the left, or only the right dataset, that do
  not have a matching row in the other dataset.
* We must be able to return all rows that do not appear in both datasets.

## Tools for Implementation

The tool will be written in Python, and will use the following libraries. If necessary
it is acceptable to add additional dependencies that can be installed from `conda-forge`
(preferred) or PyPI.

### Polars (and DuckDB, if necessary) for Tabular Data

Due to the size of some of the PUDL data, and the fact that reference datasets will
typically be stored in cloud buckets as Apache Parquet outputs, we will use either
Polars dataframes or DuckDB to query, load, manipulate, and compare the data.
These libraries are fast, memory efficient, do aggressive query optimization and lazy
execution, can handle large data, are internally parallelized, and are able to query
remote Parquet files directly without needing to download them first. Our team is more
proficient in Python and Polars in general, so we will use Polars as the primary tool
for this work, and only fall back on DuckDB if we run into a situation where Polars is
unable to do what we need it to do.

In interactive or programmatic uses, the functions that return data should be able to
return either pandas or polars dataframes, depending on user preferences, since we are
still more familiar with pandas than Polars as a team.

### Marimo Notebooks for Interactive Exploration and Visualization

We will create a shared Marimo notebook at `notebooks/pudl-diff.py` that uses the
functions defined above to do dataset comparisons & visualizations interactively. Like
the underlying functions, the notebook will need to have tests that run in CI to ensure
that it works correctly and produces the expected outputs.

In the notebook allow the user to enter two root paths and the table to be compared. The
notebook will generate a report including samples of data and visualizations describing
the diffs between the two tables. Initially, the notebook should be runnable locally
with `pixi run marimo edit notebooks/pudl-diff.py` Eventually we will host this notebook
somewhere privately and give it access to
[`builds.catalyst.coop`](http://builds.catalyst.coop) so we can check on the diffs
between a build and nightly if something is weird. Eventually we will also want to host
it in a public place like our `pudl-examples` repository to allow users to explore the
differences between nightly and stable PUDL outputs, or between different historical
versioned releases.

### Click will be used to create a CLI tool

We will use the Click framework to create a new CLI at `src/pudl/scripts/pudl_diff.py`.
The CLI will generate a short formatted text summary of the differences between two versions
of the same table using the above functionality. The CLI will be intended for interactive
and will help users answer decide whether they need to open up the
`notebooks/pudl-diff.py` notebook to explore the differences in more detail.

### As a CI Tool

Eventually we intend to use the CLI to generate a nightly data diff report as part of
the nightly builds. For this application we will add an option to the CLI to summarize
the difference between two whole sets of PUDL Parquet outputs representing the full
outputs of a PUDL ETL run. The report will be saved to
[`builds.catalyst.coop`](http://builds.catalyst.coop).

Typically we expect the diffs between the current build outputs and the previous nightly
build outputs to be small, given that only a few tables or one dataset at a time gets
updated. Depending on how voluminous it really is we can dial the detail up or down.

We may also want to have the script use the Marimo notebook and export the results as
HTML so we have a clickable nightly data diff report. We could make these reports
available online for the public to view so users can understand how the data outputs are
changing over time. We could save them as a record of the shape of the nightly build
outputs for forensic / debugging purposes.

## Positive Impacts on the PUDL Project

* `pudl-diff` will give us the ability to make relatively sweeping refactors that are
  **not** intended to change data outputs confidently. For example, we might have
  a coding agent rewrite one or more of our existing assets to use Polars instead of
  Pandas, or to otherwise improve performance, without changing behavior. We could
  `pudl-diff` tooling to verify that the data outputs are functionally identical before
  and after the refactor.
* Similarly, we would be able to update core packages like `pandas`, `polars`, `duckdb`,
  or `splink` and use coding agents to address breaking changes in the APIs during major
  version updates, and then use `pudl-diff` to verify that the data outputs are
  functionally identical before and after the refactoring.
* This tooling will also give us fine-grained and reproducible insight into exactly
  **what** has changed when the data changes – beyond the basic facts of schema
  conformance and row-count stability. This will catch data processing errors early
  before we publish the bad data, and make debugging those errors much faster and easier.
* This kind of data regression test can also provide another kind of oracle for coding
  agents that are working on our codebase day-to-day. If we are asking them to do a
  performance or library migration refactor or a bugfix and we **know** what the diff
  should look like, or that there should not be one, that expectation can be encoded in
  our instructions to the agent and used as a guardrail in the same way that tests, type
  checking, linting, etc. are today, allowing them to iterate on the task with less
  supervision.

## Prior Work

For reference and some implementation ideas, prior efforts to compare PUDL outputs
include:

* `devtools/check_against_nightly.py`
* `devtools/sqlite-table-diff.ipynb`
* `devtools/inspect-assets.ipynb`
* Changes on the 3-year old `rousik-output-diff` branch in the PUDL repo.

## Implementation Plan

* We will do this work in a series of discrete, incremental steps that build upon each
  other.
* All changes will be reviewed and committed by me. You will not make any commits
  yourself.
* After each task you will write an appropriate commit message describing the last set
  of changes and save it to a file called commit.txt for me to review. Do not sign the
  commit message.
* Ask me for additional context and feedback at any time if you are unsure about how to
  proceed with a task or there is a major design decision with multiple reasonable
  options.

### Core Functionality

* We will start by implementing the core functionality in `src/pudl/validate/diff.py`
  and writing the associated unit tests.
* Given the context above and whatever additional review of the PUDL codebase and
  documentation you need to do, you will propose a set of well-defined tasks to
  implement the core functionality.
* I will review and approve that plan before you start working on them.

#### Approved task breakdown

Pure data-comparison library, no CLI/notebook yet. Two root paths (local or S3, via
`UPath`), each containing Parquet files and a `datapackage.json`. Uses Polars as the
primary tool, with pandas conversion available on demand, falling back to DuckDB only
if Polars can't do what's needed. Data visualization (scatterplots, heatmaps) is
deferred to the Marimo notebook phase — Core Functionality only produces the
comparison data, not plots. Float comparisons default to `numpy.isclose()` defaults
(`rtol=1e-5, atol=1e-8`), with override parameters, refined later based on initial
results.

**Task 1 — Dataset/table loading primitives**

* A dataset wrapper (e.g. `PudlDiffDataset`) around a root `UPath` plus a cached
  `Package` parsed from `datapackage.json`, reusing `pudl.metadata.classes.Package`
  and `Resource`.
* A method to lazily load a named table as a `pl.LazyFrame` from
  `<root>/<table>.parquet`, working for both local and S3 roots without downloading
  remote files where Polars supports it directly (e.g. `pl.scan_parquet` with
  `storage_options`).
* A method to look up a table's `Resource` and expose its `primary_key`
  (`list[str]`, possibly empty), reusing `Package.get_resource`.
* Unit tests: fixture Parquet files plus a `datapackage.json` written to `tmp_path`;
  verify loading and schema/PK lookup, including the no-primary-key case.

**Task 2 — Schema comparison**

* Compare two `pl.Schema`/dtype mappings: same column set (order-independent),
  matching dtypes.
* Return a structured result (e.g. `SchemaDiff` dataclass with
  `columns_only_in_left`, `columns_only_in_right`,
  `dtype_mismatches: dict[str, tuple[dtype, dtype]]`) and an `is_identical`
  property.
* Unit tests: identical schemas, extra/missing columns, dtype mismatches,
  column-order independence.

**Task 3 — Row-count comparison (with and without partitioning)**

* Given two lazyframes and an optional partition column/expression, compute
  per-partition expected/observed row counts and their differences — the same
  semantics as the `check_row_counts_per_partition` dbt macro
  (`dbt/macros/row_counts_per_partition.sql`), reimplemented in Polars
  (`group_by(partition_col).len()` plus a full outer join). No partition column
  means a single implicit partition.
* Unit tests: matching counts, mismatched counts, extra/missing partitions, null
  partition values.

**Task 4 — Row-level comparison for tables without primary keys**

* Given two lazyframes with identical schemas, compute the symmetric difference of
  rows (e.g. `anti_join` in both directions, or `unique(keep="none")` after
  concatenation with a source label).
* Return rows-only-in-left, rows-only-in-right, and a combined dataframe labeled by
  source dataset; every result convertible to polars or pandas via a consistent
  `as_pandas: bool = False` parameter across all public functions.
* Since there is no primary key, exact-match anti-joins don't tolerate float noise
  on their own; apply a rounding/bucketing strategy to float columns before the
  anti-join so that "close enough" values are treated as equal. This strategy will
  be revisited based on initial real-world results.
* Unit tests: identical tables, added/removed/changed rows, float tolerance edge
  cases.

**Task 5 — Row-level comparison for tables with primary keys**

* Given PK columns, compute the symmetric difference of PK values (anti-joins on PK
  columns), returning left-only/right-only/combined rows with a source label.
* For shared PKs, join on PK and compare non-PK columns per-column with type-aware
  equality (float columns via `numpy.isclose`-style tolerance, others exact),
  returning: an overall identical bool, a per-column mismatch summary, and a
  dataframe of mismatched rows (or a sample) with both left/right values available
  for inspection (e.g. suffixed columns).
* Unit tests: identical PK sets, differing PK sets (left-only, right-only, both),
  identical PKs with identical vs. differing non-PK data (numeric, string/
  categorical, float-tolerance).

**Task 6 — Top-level orchestration**

* A single `compare_table(left_root, right_root, table_name, partition_col=None) ->
  TableDiffResult` tying together Tasks 2-5: runs the schema diff and row-count
  diff, dispatches to PK-based or PK-less row comparison depending on the table's
  schema, and reports overall `is_identical` per the "functionally identical"
  definition above.
* Unit tests: end-to-end fixture-based tests exercising the full result object for
  a PK'd table and a PK-less table, both identical and differing.

### The PUDL Diff Marimo Notebook

We are not yet ready to implement the Marimo notebook.

### The PUDL Diff CLI Tool

We are not yet ready to implement the CLI tool.

### Nightly PUDL Data Diff Reporting

We are not yet ready to implement nightly data diff reporting.
