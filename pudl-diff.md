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

### Core Functionality (3hr38m on 2026-09-16)

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

#### Follow-up: strategy for large tables

Smoke-testing `compare_table` against real PUDL output revealed that the current
row-level comparison functions (`compare_rows_with_pk`, `compare_rows_without_pk`)
materialize full joined dataframes in memory rather than streaming. This works fine
up to the tens-of-millions-of-rows range, but comparing `out_vcerare__hourly_available_capacity_factor`
(300M rows) against itself peaked at ~93GB RSS, and `core_epacems__hourly_emissions`
(~1.02B rows) OOM-killed the process outright on a 128GB machine.

As a stopgap, `compare_table` now bails out of row-level comparison (logging a
warning, still running the cheap schema and row-count comparisons) whenever either
side of a table exceeds `MAX_ROWS_FOR_ROW_LEVEL_COMPARISON` (100,000,000 rows). We
need to follow up with an actual strategy for comparing these large tables' row
contents without exhausting memory — candidates include a streaming/lazy `sink_*`-based
implementation, chunking the comparison (e.g. by the same partition column used for
row counts), or falling back to DuckDB's out-of-core execution as anticipated in the
"Tools for Implementation" section above. Not yet scheduled as a task.

#### Smoke test performance results

`compare_table(ds, ds, table_name)` run against a real local PUDL build, comparing
each table to itself (expected result: identical). All machine specs: 128GB RAM.
Rows above the double rule were re-run in their own isolated process to get an
accurate per-table peak RSS (each pays a ~250MB baseline for the Python/Polars
process itself, on top of which the comparison's own memory shows up as the
table grows); the two large tables below that were only run once each per
scenario, since re-running them repeatedly isn't cheap.

| Table | Rows | Primary key | Time | Peak RSS | Result |
| --- | ---: | :---: | ---: | ---: | --- |
| `core_eia__codes_wet_dry_bottom` | 2 | Yes | 0.008 s | 256 MB | identical |
| `core_rus__codes_investment_types` | 11 | Yes | 0.007 s | 258 MB | identical |
| `core_eia861__yearly_distributed_generation_fuel` | 24,052 | No | 0.011 s | 274 MB | identical |
| `core_eia176__yearly_gas_exports` | 12,740 | No | 0.011 s | 273 MB | identical |
| `out_rus12__yearly_investments` | 24,091 | No | 0.013 s | 289 MB | identical |
| `out_rus7__yearly_materials_and_supplies` | 16,912 | Yes | 0.021 s | 293 MB | identical |
| `_core_phmsagas__yearly_distribution_misc` | 91,983 | No | 0.020 s | 352 MB | identical |
| `core_rus12__yearly_plant_costs` | 52,500 | No | 0.022 s | 305 MB | identical |
| `core_ferc1__yearly_energy_sources_sched401` | 41,913 | Yes | 0.022 s | 325 MB | identical |
| `core_eia861__yearly_operational_data_revenue` | 462,133 | Yes | 0.094 s | 516 MB | identical |
| `_core_eia__forensics_entity_resolution_generators` | 1,758,590 | No | 0.142 s | 1,179 MB | identical |
| `out_eia923__monthly_boiler_fuel` | 1,892,184 | Yes | 0.351 s | 2,232 MB | identical |
| `core_eiaaeo__yearly_projected_energy_use_by_sector_and_type` | 1,163,063 | Yes | 0.354 s | 967 MB | identical |
| `core_eia930__hourly_subregion_demand` | 5,974,704 | Yes | 0.507 s | 2,225 MB | identical |
| `out_eia__yearly_generators_by_ownership` | 1,417,312 | No | 0.494 s | 2,421 MB | identical |
| `out_eia930__hourly_subregion_demand` | 5,974,704 | Yes | 0.573 s | 2,442 MB | identical |
| `_core_phmsagas__yearly_distribution_by_material_and_size` | 8,085,709 | No | 0.633 s | 4,295 MB | identical |
| `out_eia__yearly_plant_parts` | 5,445,543 | Yes | 1.521 s | 12,554 MB | identical* |
| `out_vcerare__hourly_available_capacity_factor` | 300,161,400 | Yes | 82.9 s | ~93 GB | identical (before the 100M-row bail-out was added; full row-level join) |
| `out_vcerare__hourly_available_capacity_factor` | 300,161,400 | Yes | 0.26 s | — | row-level comparison skipped (after the bail-out; schema + row count only) |
| `core_epacems__hourly_emissions` | 1,017,999,168 | Yes | — | OOM-killed | crashed (before the bail-out; full row-level join) |
| `core_epacems__hourly_emissions` | 1,017,999,168 | Yes | 0.79 s | — | row-level comparison skipped (after the bail-out; schema + row count only) |

\* `out_eia__yearly_plant_parts` initially reported as a false mismatch due to the
infinity-handling bugs described above; after the fix it correctly reports identical.
Its peak RSS is disproportionately high relative to its row count (~5.4M rows, on
par with `out_eia930__hourly_subregion_demand` at ~6M) because it's unusually wide
(87 columns), which drives up the memory cost of building the full left/right join
used by `compare_rows_with_pk` — a first hint that column count, not just row
count, matters for the large-table strategy noted above.

### The PUDL Diff CLI Tool (2026-09-17 19:46)

We are going to build a CLI around the `src/pudl/validate/diff.py` module.
We will use the Click framework to create a new CLI at `src/pudl/scripts/pudl_diff.py`.
The CLI will generate a structured report using JSON that summarizes the results of the diff.
The structured report will be consumed by agents that are using the CLI to compare PUDL outputs.
The structured report will also be saved to disk as a record of the diff in various contexts, including nightly builds and versioned data releases.
Later we will also use these structured reports as an input to a Marimo notebook for visualizing the results of a diff, or a collection of diffs.
We will also add a CLI option to generate a human-readable summary of the diff, which will be useful for interactive use and for debugging.
However, for now we are focused just on the structured JSON report, which will enable those other applications.
First we will define the structure of the report for a single pair of tables, and later create a higher level structure for comparing two entire datasets composed of many tables.

#### Information contained in the PUDL Diff report structure

The table diff summary report will be a JSON object, serialized from a Pydantic
`TableDiffReport` model (so it can be validated on reload for later analysis and
visualization, rather than a plain dataclass).
The JSON object will not contain any of either table's actual data.
Alongside the JSON report we will also save a pair of Parquet files.
The parquet files will contain left-only and right-only rows, respectively, for tables that have primary keys.
The parquet files will contain the rows that are part of the symmetric difference of the two tables, and also the rows that have matching primary keys but differing non-PK data.
For tables that do not have primary keys, the parquet files will contain the rows that are part of the symmetric difference of the two tables.
The schema of the left-only Parquet file must be identical to the schema of the left table, and the schema of the right-only Parquet file must be identical to the schema of the right table.

The report will contain the following information for each table comparison:

* Report provenance
  * Report creation timestamp (UTC, ISO-8601), matching the `created` field
    convention used in PUDL's enriched `datapackage.json`
  * Left dataset provenance: `id`, `created`, `git_sha`, `git_tags`, read
    from the left dataset's own `datapackage.json` if present (omitted/null
    for any field that dataset's descriptor doesn't have, e.g. an older
    build without git provenance) — `created` here is the left dataset's own
    build timestamp, distinct from the report creation timestamp above
  * Right dataset provenance: same four fields, read from the right
    dataset's `datapackage.json`
* Table level information
  * Left table name (string)
  * Path to the left table input file (local path or URL)
  * Right table name (string)
  * Path to the right table input file (local path or URL)
  * Overall table identical boolean (True if the two tables are functionally identical, False otherwise)
  * Time it took to run the comparison (in seconds)
  * Peak memory usage during the comparison (in bytes)
  * Peak CPU utilization during the comparison (percent of one core, e.g.
    400.0 for four cores kept fully busy at once)
* Schema comparison results
  * Columns only in left table
  * Columns only in right table
  * Dtype mismatches (column name, left dtype, right dtype)
  * Overall schema identical boolean
* Row count comparison results
  * Total rows in left table
  * Total rows in right table
  * Row count difference (left - right)
  * Partitioned row counts (if applicable)
    * Partition column name
    * Partition values and their respective row counts in left and right datasets
    * Row count differences per partition
  * Overall row count identical boolean
* Row-level comparison results (if applicable)
  * Primary key presence and comparison results
    * Left and right table primary key columns
    * Number of primary keys found only in left table
    * Number of primary keys found only in right table
    * Primary key set identical boolean (True if the two tables have the same set of primary keys, False otherwise)
    * Number of rows with matching primary keys but differing non-PK data (if applicable)
    * Column-wise mismatch summary (column name, number of differing values in that column, across all the rows with matching primary keys but at least one differing non-PK value)
  * Non-primary key row-level comparison results (if applicable)
    * Number of rows only in left table
    * Number of rows only in right table
    * Symmetric difference row count (sum of the above two counts)
    * Overall row-level identical boolean (True if the two tables have the same set of rows, False otherwise)
  * If row-level comparison did not run, a reason (too many rows, incompatible dtypes, or mismatched columns without a usable primary key)
  * Left-only Parquet output: path, `bytes`, `hash` (`"sha256:<hexdigest>"`,
    matching PUDL's enriched `datapackage.json` resource convention)
  * Right-only Parquet output: same three fields
* Error summary (if any errors occurred during the comparison, including error messages and stack traces)
* Success boolean (True if the comparison completed successfully, False otherwise)

#### PUDL Diff CLI Arguments

* Path to the left dataset root (local path or URL, optional, defaults to `$PUDL_OUTPUT/parquet`)
* Path to the right dataset root (local path or URL, optional, defaults to `s3://pudl.catalyst.coop/nightly/`)
* Left table name to compare (string, required)
* Right table name to compare (string, optional, defaults to left table name)
* Output path for the JSON report and Parquet outputs (local path, optional, defaults to current working directory, must be a writable directory, will be created along with parent directories if it does not yet exist)
* Max table size to compare at the row level (integer, optional, defaults to 100,000,000)
* Max number of rows to save to each Parquet output file (integer, optional, defaults to all rows)

#### Approved task breakdown

Human-readable text-report formatting is deferred until after the JSON report
works; not scheduled as a task yet.

**Task 0 — Restructure the row-level diff data model**

* Split `compare_rows_with_pk`'s single suffixed `mismatched_rows` dataframe
  into separate `mismatched_left`/`mismatched_right` DataFrames, each holding
  the PK columns plus the original (non-suffixed) column names with just
  that side's values — selected off the same shared-PK inner join used
  today, so nothing ever materializes a doubled-width combined frame. This
  also matches the Parquet output shape directly, with no un-suffixing
  needed in Task 4.
* Remove `RowSetDiff.combined` and the module-level `SOURCE_COL` constant —
  nothing consumes the label-tagged concatenation, and it doesn't match the
  separate-left/right-file convention the report and Parquet outputs use.
* Update `KeyedRowDiff` accordingly: `pk_diff: RowSetDiff`,
  `column_mismatches: dict[str, int]`, `mismatched_left`, `mismatched_right`.
* Update all existing unit tests in `diff_test.py` that reference
  `.combined`, `SOURCE_COL`, or `mismatched_rows` to match the new shape.

**Task 1 — Row-count totals, skip-reason, and a configurable row-level cap**

* Add `left_row_count`/`right_row_count` totals to `RowCountDiff` (currently
  it only tracks per-partition mismatches, not the overall totals the report
  needs).
* Add a `row_diff_skipped_reason: str | None` field to `TableDiffResult`,
  populated by `compare_table` with why `row_diff` is `None`: too many rows,
  dtype-incompatible join failure, or mismatched columns (with or without a
  usable primary key).
* Turn `MAX_ROWS_FOR_ROW_LEVEL_COMPARISON` into a `compare_table` parameter
  (`max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON`)
  so the CLI can override it, keeping the module constant as the default.
* Unit tests: one case per skip reason, row-count totals on partitioned and
  unpartitioned comparisons, and an explicit override of the row-level cap.

**Task 2 — Timing and peak-memory/CPU instrumentation**

* Add `elapsed_seconds: float`, `peak_rss_bytes: int`, and
  `peak_cpu_percent: float` fields to `TableDiffResult`, measured by
  `compare_table` around its own execution.
* Peak RSS and CPU measured via a `psutil`-based background sampler thread
  (polls `psutil.Process().memory_info().rss` and
  `psutil.Process().cpu_percent()` at a short interval, tracks the max of
  each) rather than `resource.getrusage`, avoiding the whole-process/high-
  water-mark and macOS-vs-Linux unit issues; RSS is netted out against a
  pre-call baseline, while CPU percent is a percentage of one core (e.g.
  `400.0` for four cores kept fully busy at once).
* Add `psutil` to `[tool.pixi.dependencies]` in `pyproject.toml`; run
  `pixi install`.
* Unit tests: elapsed time is positive; sampler correctness tested against a
  mocked/fake memory and CPU source rather than relying on real allocation
  timing.

**Task 3 — Error-tolerant comparison wrapper**

* A new outer function (e.g. `run_table_diff`) that wraps `compare_table`,
  catching any exception raised during the comparison and capturing it as an
  error summary (message + traceback) plus a `success: bool`, so a report can
  always be produced even when the comparison itself blows up (e.g. a
  missing datapackage, an S3 access failure) — distinct from the existing
  in-`compare_table` handling of dtype-incompatible row joins, which is an
  expected skip, not an error.
* Unit tests: a simulated failure (e.g. missing datapackage) yields
  `success=False` with a captured error; a normal run yields `success=True`
  with no error.

**Task 4 — Parquet side-output writer**

* A new function, e.g. `write_row_diff_parquet(row_diff, output_path, ...)`,
  that writes `<table_name>_left_only.parquet` / `..._right_only.parquet`
  (schema-matched to the left/right tables respectively) for both the PK
  case (`KeyedRowDiff`: combines `pk_diff.only_in_left`/`only_in_right` with
  `mismatched_left`/`mismatched_right` from Task 0) and the non-PK case
  (`RowSetDiff`: `only_in_left`/`only_in_right` directly).
* Applies the `--max-rows-per-output-parquet` cap, while still recording the
  true total row count (for the JSON report) separately from what was
  written to disk.
* After writing, computes each file's `bytes` (size) and `hash`
  (`"sha256:<hexdigest>"` via `hashlib.sha256`, matching the convention in
  `src/pudl/dagster/assets/core/datapackage.py`) for the report's provenance
  section.
* Unit tests: PK table (symmetric-diff + mismatched rows land on the correct
  side with the original schema), non-PK table, row capping behavior, correct
  `bytes`/`hash` values, and the no-row-diff (skipped) case writing nothing.

**Task 5 — JSON report structure and serializer**

* A new `TableDiffReport` Pydantic model (with nested models for each
  section) matching the detailed schema above, including the provenance
  section (report `created` timestamp; left/right dataset `id`/`created`/
  `git_sha`/`git_tags`, read via a new `PudlDiffDataset.provenance()` method
  that pulls those optional fields from `self.datapackage`) and the Parquet
  output `path`/`bytes`/`hash` fields from Task 4 — with no row-level data
  embedded, only counts and summaries.
* Built from a `run_table_diff` result plus the two datasets' table
  names/paths and provenance.
* `model_dump_json(indent=2)` used for serialization (no separate custom
  serializer needed, unlike the dataclass-based approach originally
  considered).
* Unit tests: report structure for an identical PK table, a differing PK
  table, a differing non-PK table, a schema-mismatch case, a skipped-large-
  table case, an error case, and a dataset with/without git provenance in
  its `datapackage.json` — each checked against the spec's field list.

**Task 6 — CLI: arguments, orchestration, exit codes**

* New `src/pudl/scripts/pudl_diff.py`: required `table_name` argument;
  `--left`/`--right` defaulting to `PudlPaths().parquet_path()` and
  `pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH`; `--right-table-name`;
  `--output-path` (default cwd, created if missing); `--max-rows-for-row-
  level-comparison`; `--max-rows-per-output-parquet`; `--rtol`/`--atol`;
  `--partition-col`/`--no-auto-partition`.
* Orchestrates `run_table_diff` → Parquet side-output writing → JSON report
  serialization → write to `<output-path>/<table_name>_diff.json`.
* Exit codes: `0` identical, `1` not identical, `2` error (unknown table,
  dataset load failure not otherwise caught by `run_table_diff`, e.g. bad
  CLI arguments).
* Register `pudl_diff = "pudl.scripts.pudl_diff:main"` in
  `[project.scripts]`.
* Unit tests: end-to-end CliRunner tests against fixture datasets on
  `tmp_path`, covering identical, differing (PK and non-PK), unknown table,
  large-table skip, and a simulated error — checking exit codes and the
  written JSON/Parquet files' contents.

**Task 7 — Docs and release notes**

* Add a release notes entry to `docs/release_notes.rst` with issue/PR
  numbers.
* Confirm the "The PUDL Diff CLI Tool" section of `pudl-diff.md` reflects
  actual usage now that the tool exists (update if behavior diverged during
  implementation).

### The PUDL Diff Marimo Notebook

We are not yet ready to implement the Marimo notebook.

### Nightly PUDL Data Diff Reporting

We are not yet ready to implement nightly data diff reporting.
