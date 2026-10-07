# PUDL Diff: A tool for comparing PUDL outputs

!!! note "A historical document"

    This is the plan that `pudl_diff` was built from, and the log of the decisions made along the way, as it was kept while the tool was developed.
    It was written when the tool was still part of the PUDL repository, so the paths and names in it (`src/pudl/validate/diff.py`, `pudl.scripts.pudl_diff`, `docs/dev/pudl_diff.rst`) are those of that time, and some of the plans in it, like the comparison of row counts by partition, were later dropped.
    The [changelog](changelog.md) is a shorter account of the same history, with links to the commits, and the [usage](usage.md) page and [API reference][pudl_diff] describe the tool as it is now.

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
default we will compare the outputs of the most recent successful nightly build at
`s3://pudl.catalyst.coop/nightly/` (the "left"/reference dataset) against the local
PUDL dataset at `$PUDL_OUTPUT/parquet` (the "right" dataset), so the diff reads as
what's changed locally since the last nightly build. However, users will be able to
override these defaults and provide any two root paths to compare. In addition to
the root path, most
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

### At the table level

- Are the two instances of the same table functionally identical?
- If the column dtypes are not identical, how do they differ?
- If the number of rows is not identical, how do they differ, and within which
    partitions of the table are the differences to be found? This question is equivalent
    to our `check_row_counts_by_partition` dbt data test.

### For Tables with Primary Keys

- We will use the PUDL schema contained in the `datapackage.json` descriptor to
    determine which tables have primary keys, and what those primary keys are.
- Do the two instances of the same table share the same set of primary keys?
    - If not, what is the symmetric difference of their primary keys?
    - If so, are the contents of the non-PK columns functionally identical?
- If the two instances of the same table have **different** primary keys:
    - What is the [**symmetric
        difference**](https://en.wikipedia.org/wiki/Symmetric_difference) of their primary
        keys?
    - What do the rows that are part of the symmetric difference contain?
    - We must be able to return these rows as a pandas or polars dataframe
    - We must be able to label the rows according to which dataset they came from
    - We must be able return the rows from only the left, or only the right dataset, that
        do not have a matching primary key in the other dataset.
    - We must be able to return all rows associated with primary keys that do not appear
        in both datasets.
- For tables in which the primary keys are **identical**
    - Are the contents of the non-PK columns functionally identical?
    - If not, we must be able to summarize how a given data column is different.
        - For numerical columns, we will need to generate an X-Y scatterplot.
        - For categorical columns, we will need to generate an X-Y heatmap (where each axis
            is the set of unique values in the column, and the color of each cell is the
            number of rows that have that combination of values).
    - If not, we must be able to show a sample (or all) of the rows that aren’t identical.

### For Tables without Primary Keys

- Are the two instances of the same table functionally identical?
- If not, we must be able to select and return only the rows that are part of the
    symmetric difference, with a label indicating which dataset they came from.
- We must be able to return these rows as a pandas or polars dataframe.
- We must be able to label the rows according to which dataset they came from.
- We must be able return the rows from only the left, or only the right dataset, that do
    not have a matching row in the other dataset.
- We must be able to return all rows that do not appear in both datasets.

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

- `pudl-diff` will give us the ability to make relatively sweeping refactors that are
    **not** intended to change data outputs confidently. For example, we might have
    a coding agent rewrite one or more of our existing assets to use Polars instead of
    Pandas, or to otherwise improve performance, without changing behavior. We could
    `pudl-diff` tooling to verify that the data outputs are functionally identical before
    and after the refactor.
- Similarly, we would be able to update core packages like `pandas`, `polars`, `duckdb`,
    or `splink` and use coding agents to address breaking changes in the APIs during major
    version updates, and then use `pudl-diff` to verify that the data outputs are
    functionally identical before and after the refactoring.
- This tooling will also give us fine-grained and reproducible insight into exactly
    **what** has changed when the data changes – beyond the basic facts of schema
    conformance and row-count stability. This will catch data processing errors early
    before we publish the bad data, and make debugging those errors much faster and easier.
- This kind of data regression test can also provide another kind of oracle for coding
    agents that are working on our codebase day-to-day. If we are asking them to do a
    performance or library migration refactor or a bugfix and we **know** what the diff
    should look like, or that there should not be one, that expectation can be encoded in
    our instructions to the agent and used as a guardrail in the same way that tests, type
    checking, linting, etc. are today, allowing them to iterate on the task with less
    supervision.

## Prior Work

For reference and some implementation ideas, prior efforts to compare PUDL outputs
include:

- `devtools/check_against_nightly.py`
- `devtools/sqlite-table-diff.ipynb`
- `devtools/inspect-assets.ipynb`
- Changes on the 3-year old `rousik-output-diff` branch in the PUDL repo.

## Implementation Plan

- We will do this work in a series of discrete, incremental steps that build upon each
    other.
- All changes will be reviewed and committed by me. You will not make any commits
    yourself.
- After each task you will write an appropriate commit message describing the last set
    of changes and save it to a file called commit.txt for me to review. Do not sign the
    commit message.
- Ask me for additional context and feedback at any time if you are unsure about how to
    proceed with a task or there is a major design decision with multiple reasonable
    options.

### Core Functionality

- We will start by implementing the core functionality in `src/pudl/validate/diff.py`
    and writing the associated unit tests.
- Given the context above and whatever additional review of the PUDL codebase and
    documentation you need to do, you will propose a set of well-defined tasks to
    implement the core functionality.
- I will review and approve that plan before you start working on them.

#### Approved task breakdown

Pure data-comparison library, no CLI/notebook yet. Two root paths (local or S3, via
`UPath`), each containing Parquet files and a `datapackage.json`. Uses Polars as the
primary tool, with pandas conversion available on demand, falling back to DuckDB only
if Polars can't do what's needed. Data visualization (scatterplots, heatmaps) is
deferred to the Marimo notebook phase — Core Functionality only produces the
comparison data, not plots. Float comparisons default to `numpy.isclose()` defaults
(`rtol=1e-5, atol=1e-8`), with override parameters, refined later based on initial
results.

##### Task 1 — Dataset/table loading primitives

- A dataset wrapper (e.g. `PudlDiffDataset`) around a root `UPath` plus a cached
    `Package` parsed from `datapackage.json`, reusing `pudl.metadata.classes.Package`
    and `Resource`.
- A method to lazily load a named table as a `pl.LazyFrame` from
    `<root>/<table>.parquet`, working for both local and S3 roots without downloading
    remote files where Polars supports it directly (e.g. `pl.scan_parquet` with
    `storage_options`).
- A method to look up a table's `Resource` and expose its `primary_key`
    (`list[str]`, possibly empty), reusing `Package.get_resource`.
- Unit tests: fixture Parquet files plus a `datapackage.json` written to `tmp_path`;
    verify loading and schema/PK lookup, including the no-primary-key case.

##### Task 2 — Schema comparison

- Compare two `pl.Schema`/dtype mappings: same column set (order-independent),
    matching dtypes.
- Return a structured result (e.g. `SchemaDiff` dataclass with
    `columns_only_in_left`, `columns_only_in_right`,
    `dtype_mismatches: dict[str, tuple[dtype, dtype]]`) and an `is_identical`
    property.
- Unit tests: identical schemas, extra/missing columns, dtype mismatches,
    column-order independence.

##### Task 3 — Row-count comparison (with and without partitioning)

- Given two lazyframes and an optional partition column/expression, compute
    per-partition expected/observed row counts and their differences — the same
    semantics as the `check_row_counts_per_partition` dbt macro
    (`dbt/macros/row_counts_per_partition.sql`), reimplemented in Polars
    (`group_by(partition_col).len()` plus a full outer join). No partition column
    means a single implicit partition.
- Unit tests: matching counts, mismatched counts, extra/missing partitions, null
    partition values.

##### Task 4 — Row-level comparison for tables without primary keys

- Given two lazyframes with identical schemas, compute the symmetric difference of
    rows (e.g. `anti_join` in both directions, or `unique(keep="none")` after
    concatenation with a source label).
- Return rows-only-in-left, rows-only-in-right, and a combined dataframe labeled by
    source dataset; every result convertible to polars or pandas via a consistent
    `as_pandas: bool = False` parameter across all public functions.
- Since there is no primary key, exact-match anti-joins don't tolerate float noise
    on their own; apply a rounding/bucketing strategy to float columns before the
    anti-join so that "close enough" values are treated as equal. This strategy will
    be revisited based on initial real-world results.
- Unit tests: identical tables, added/removed/changed rows, float tolerance edge
    cases.

##### Task 5 — Row-level comparison for tables with primary keys

- Given PK columns, compute the symmetric difference of PK values (anti-joins on PK
    columns), returning left-only/right-only/combined rows with a source label.
- For shared PKs, join on PK and compare non-PK columns per-column with type-aware
    equality (float columns via `numpy.isclose`-style tolerance, others exact),
    returning: an overall identical bool, a per-column mismatch summary, and a
    dataframe of mismatched rows (or a sample) with both left/right values available
    for inspection (e.g. suffixed columns).
- Unit tests: identical PK sets, differing PK sets (left-only, right-only, both),
    identical PKs with identical vs. differing non-PK data (numeric, string/
    categorical, float-tolerance).

##### Task 6 — Top-level orchestration

- A single `compare_table(left_root, right_root, table_name, partition_col=None) -> TableDiffResult` tying together Tasks 2-5: runs the schema diff and row-count
    diff, dispatches to PK-based or PK-less row comparison depending on the table's
    schema, and reports overall `is_identical` per the "functionally identical"
    definition above.
- Unit tests: end-to-end fixture-based tests exercising the full result object for
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

| Table                                                         |          Rows | Primary key |    Time |   Peak RSS | Result                                                                     |
| ------------------------------------------------------------- | ------------: | :---------: | ------: | ---------: | -------------------------------------------------------------------------- |
| `core_eia__codes_wet_dry_bottom`                              |             2 |     Yes     | 0.008 s |     256 MB | identical                                                                  |
| `core_rus__codes_investment_types`                            |            11 |     Yes     | 0.007 s |     258 MB | identical                                                                  |
| `core_eia861__yearly_distributed_generation_fuel`             |        24,052 |     No      | 0.011 s |     274 MB | identical                                                                  |
| `core_eia176__yearly_gas_exports`                             |        12,740 |     No      | 0.011 s |     273 MB | identical                                                                  |
| `out_rus12__yearly_investments`                               |        24,091 |     No      | 0.013 s |     289 MB | identical                                                                  |
| `out_rus7__yearly_materials_and_supplies`                     |        16,912 |     Yes     | 0.021 s |     293 MB | identical                                                                  |
| `_core_phmsagas__yearly_distribution_misc`                    |        91,983 |     No      | 0.020 s |     352 MB | identical                                                                  |
| `core_rus12__yearly_plant_costs`                              |        52,500 |     No      | 0.022 s |     305 MB | identical                                                                  |
| `core_ferc1__yearly_energy_sources_sched401`                  |        41,913 |     Yes     | 0.022 s |     325 MB | identical                                                                  |
| `core_eia861__yearly_operational_data_revenue`                |       462,133 |     Yes     | 0.094 s |     516 MB | identical                                                                  |
| `_core_eia__forensics_entity_resolution_generators`           |     1,758,590 |     No      | 0.142 s |   1,179 MB | identical                                                                  |
| `out_eia923__monthly_boiler_fuel`                             |     1,892,184 |     Yes     | 0.351 s |   2,232 MB | identical                                                                  |
| `core_eiaaeo__yearly_projected_energy_use_by_sector_and_type` |     1,163,063 |     Yes     | 0.354 s |     967 MB | identical                                                                  |
| `core_eia930__hourly_subregion_demand`                        |     5,974,704 |     Yes     | 0.507 s |   2,225 MB | identical                                                                  |
| `out_eia__yearly_generators_by_ownership`                     |     1,417,312 |     No      | 0.494 s |   2,421 MB | identical                                                                  |
| `out_eia930__hourly_subregion_demand`                         |     5,974,704 |     Yes     | 0.573 s |   2,442 MB | identical                                                                  |
| `_core_phmsagas__yearly_distribution_by_material_and_size`    |     8,085,709 |     No      | 0.633 s |   4,295 MB | identical                                                                  |
| `out_eia__yearly_plant_parts`                                 |     5,445,543 |     Yes     | 1.521 s |  12,554 MB | identical\*                                                                |
| `out_vcerare__hourly_available_capacity_factor`               |   300,161,400 |     Yes     |  82.9 s |     ~93 GB | identical (before the 100M-row bail-out was added; full row-level join)    |
| `out_vcerare__hourly_available_capacity_factor`               |   300,161,400 |     Yes     |  0.26 s |          — | row-level comparison skipped (after the bail-out; schema + row count only) |
| `core_epacems__hourly_emissions`                              | 1,017,999,168 |     Yes     |       — | OOM-killed | crashed (before the bail-out; full row-level join)                         |
| `core_epacems__hourly_emissions`                              | 1,017,999,168 |     Yes     |  0.79 s |          — | row-level comparison skipped (after the bail-out; schema + row count only) |

\* `out_eia__yearly_plant_parts` initially reported as a false mismatch due to the
infinity-handling bugs described above; after the fix it correctly reports identical.
Its peak RSS is disproportionately high relative to its row count (~5.4M rows, on
par with `out_eia930__hourly_subregion_demand` at ~6M) because it's unusually wide
(87 columns), which drives up the memory cost of building the full left/right join
used by `compare_rows_with_pk` — a first hint that column count, not just row
count, matters for the large-table strategy noted above.

### The PUDL Diff CLI Tool

We built a CLI around the `src/pudl/validate/diff.py` module.
It uses the Click framework and lives at `src/pudl/scripts/pudl_diff.py`.
The CLI compares any number of tables between two PUDL Parquet datasets (by default, every table present in both) and writes a single structured JSON report summarizing the results of the whole comparison.
The structured report is consumed by agents that are using the CLI to compare PUDL outputs.
It is also saved to disk as a record of the diff in various contexts, including nightly builds and versioned data releases.
Later we will also use these structured reports as an input to a Marimo notebook for visualizing the results of a diff, or a collection of diffs.
The CLI also prints a human-readable, colorized summary: a line for each table as it is compared, and (when comparing more than one table) totals at the end.
Most of the logic lives in the library module rather than the CLI: the CLI only decides which tables to compare, renders the report for humans, and sets the exit code.

The report is dataset-centric.
It is built by `build_pudl_diff_report()` into a Pydantic `PudlDiffReport` model, which contains a `TableDiffReport` for each table compared.
(Pydantic, rather than plain dataclasses, so the JSON can be validated on reload for later analysis and visualization.)
The fields that pertain to the comparison as a whole are on the `PudlDiffReport`, and everything that pertains to only one table is on its `TableDiffReport`.

#### Information contained in the PUDL Diff report structure

The report is written to `pudl_diff_report.json` in the output directory, and is versioned by a `schema_version` field (currently `1.0.0`).
Before this branch merges we still need to fully document the report schema.
The JSON does not contain any of either table's actual data.
Alongside the JSON report we also save a pair of Parquet files for each table that differs.
The parquet files contain left-only and right-only rows, respectively, for tables that have primary keys.
The parquet files contain the rows that are part of the symmetric difference of the two tables, and also the rows that have matching primary keys but differing non-PK data.
For tables that do not have primary keys, the parquet files contain the rows that are part of the symmetric difference of the two tables.
The schema of the left-only Parquet file is identical to the schema of the left table, and the schema of the right-only Parquet file is identical to the schema of the right table.

##### Design decisions on the dataset-level structure

- The tables are a dict keyed by table name, not a list, so consumers can look a table up directly. The key is the table's name in the left dataset; `--right-table` only applies to a single table, so keys can't collide, and each entry still records both names.
- Report-wide fields (creation time, dataset provenance, success) live once at the top level, and were removed from the per-table reports.
- A `summary` block does the aggregation across tables (counts, row and column totals, sizes), so downstream consumers, and the CLI, don't have to.
- Every byte count is stored as an integer alongside a human-readable string (`B`, `KB`, `MB` or `GB`, in decimal units), so the numbers are easy to read in the JSON as well.
- Derived fields (the size strings, the byte difference and its percentage) are Pydantic computed fields, so they can't disagree with the byte counts they're derived from.

**Top level of the report** (`PudlDiffReport`)

- `schema_version`
- Report creation timestamp `created` (UTC, ISO-8601), matching the `created` field convention used in PUDL's enriched `datapackage.json`, and `elapsed_seconds` for the whole comparison
- `left_dataset` and `right_dataset`: the dataset's `root` (local path or URL), plus its provenance `id`, `created`, `git_sha`, `git_tags`, read from its own `datapackage.json` if present (null for any field that dataset's descriptor doesn't have, e.g. an older build without git provenance). `created` here is the dataset's own build timestamp, distinct from the report creation timestamp above.
- `options`: the settings the comparison ran with: `rtol`, `atol`, `max_compare_rows`, `max_output_rows`, `auto_partition` and `partition_expr`
- `tables_only_in_left` and `tables_only_in_right`: tables that were not compared because they are in only one dataset
- `summary` (`PudlDiffSummary`), totals over all the tables:
    - Counts of tables compared, identical, changed and failed; the names of the failed tables and of those whose schema changed
    - Total left and right row counts; rows added, changed and removed, summed over the tables that had a row-level comparison; and how many tables (and left rows) had none
    - Columns added, columns with changed dtypes, and columns removed
    - Total left and right table size, the difference, and the difference as a percentage of the left size (tables whose size is known on both sides)
    - Peak memory use of any one table, and which table it was
- `is_identical`: True only if the run succeeded and every compared table is identical. Tables in only one dataset do not count against it.
- `success`: True if the run completed and every table's comparison completed. This is distinct from `is_identical`: a comparison can succeed and find differences.
- `error`: why the run as a whole failed, if it did (e.g. the datasets have no tables in common). It is null when the only failures are of individual tables, which each record their own error.
- `tables`: the `TableDiffReport` of each table, as described below

**Each table's report** (`TableDiffReport`)

- Table level information
    - Left table name (string)
    - Path to the left table input file (local path or URL)
    - Right table name (string)
    - Path to the right table input file (local path or URL)
    - Overall table identical boolean (True if the two tables are functionally identical, False otherwise; always False if the comparison failed)
    - Time it took to run the comparison (in seconds)
    - Peak memory usage during the comparison (`peak_rss_bytes`, and `peak_rss`, human-readable)
    - Peak CPU utilization during the comparison (percent of one core, e.g. 400.0 for four cores kept fully busy at once)
- Table size, as the bytes of the Parquet file on disk or in S3/GCS (each table is a single file)
    - `left_table_bytes` and `right_table_bytes`, with `left_table_size` and `right_table_size` as human-readable strings
    - `bytes_difference`, right minus left, so negative if the table shrank, and `bytes_difference_size`, a signed human-readable string
    - `bytes_difference_percent`, as a percentage of the left size
    - Compression algorithms or levels can change these even if the table's contents don't. They are looked up on a best-effort basis even when the comparison failed, and are null when unknown (and the percentage is also null when the left size is 0).
- Schema comparison results
    - Columns only in left table
    - Columns only in right table
    - Dtype mismatches (column name, left dtype, right dtype)
    - Overall schema identical boolean
- Row count comparison results
    - Total rows in left table
    - Total rows in right table
    - Row count difference (right - left)
    - Partitioned row counts (if applicable)
        - Partition column name
        - Partition values and their respective row counts in left and right datasets
        - Row count differences per partition
    - Overall row count identical boolean
- Row-level comparison results (if applicable)
    - Primary key presence and comparison results
        - Left and right table primary key columns
        - Number of primary keys found only in left table
        - Number of primary keys found only in right table
        - Primary key set identical boolean (True if the two tables have the same set of primary keys, False otherwise)
        - Number of rows with matching primary keys but differing non-PK data (if applicable)
        - Column-wise mismatch summary (column name, number of differing values in that column, across all the rows with matching primary keys but at least one differing non-PK value)
    - Non-primary key row-level comparison results (if applicable)
        - Number of rows only in left table
        - Number of rows only in right table
        - Symmetric difference row count (sum of the above two counts)
        - Overall row-level identical boolean (True if the two tables have the same set of rows, False otherwise)
    - If row-level comparison did not run, a reason (too many rows, incompatible dtypes, or mismatched columns without a usable primary key)
    - Left-only Parquet output: path, `bytes` (and a human-readable `size`), `hash` (`"sha256:<hexdigest>"`,
        matching PUDL's enriched `datapackage.json` resource convention)
    - Right-only Parquet output: same fields
- Error summary (if the comparison of this table failed to complete, the error message and stack trace)
- Success boolean (True if the comparison of this table completed, False otherwise)

##### Terminal output

For each table the CLI prints a line with its status (`[IDENTICAL]`, `[CHANGED]` or `[ERROR]`), whether it has a primary key, column and row counts and changes (`+added/~changed/-removed`, in git-diff-like colors), the left and right sizes, the change in size and its percentage of the left size, the elapsed time, and the table name.
The size change is shown in green when the table grew and red when it shrank (gray if unchanged), like the `+`/`-` of the row and column counts.
The header is two lines, so that a column's name (e.g. `LEFT` / `COLS`, or `ROW CHANGES` over `+add/~chg/-del`) needn't make it wider than its values.
The summary at the end reads its totals from the report's `summary` and shows the number of tables identical, changed and failed, the elapsed time, peak memory, total rows and row changes, total size and change in size, column changes, the tables whose schema changed, tables with errors, and the tables in only one dataset.

#### PUDL Diff CLI Arguments

- Table names to compare (zero or more, optional). With none, compares every table that has a Parquet file in both datasets. Duplicates are compared once.
- `-l`/`--left`: path to the left dataset root (local path or URL, optional, defaults to
    `s3://pudl.catalyst.coop/nightly/`, the reference point most diffs are measured
    against)
- `-r`/`--right`: path to the right dataset root (local path or URL, optional, defaults to
    `$PUDL_OUTPUT/parquet`, so the diff reads as what's changed locally since the last
    nightly build)
- `--right-table`: right table name to compare (string, optional, defaults to the left table name; requires exactly one table name)
- `-o`/`--output-path`: directory for `pudl_diff_report.json` and the Parquet outputs (local path, optional, defaults to current working directory, will be created along with parent directories if it does not yet exist)
- `--max-compare-rows`: max table size to compare at the row level (integer, optional, defaults to 100,000,000)
- `--max-output-rows`: max number of rows to save to each Parquet output file (integer, optional, defaults to all rows)
- `--rtol` and `--atol`: tolerances for float equality
- `--partition-expr` and `--no-auto-partition`: the column to group row counts by, or turning off the automatic use of the dbt row-count partition
- `--color`/`--no-color`: colorize the output (defaults to on if stdout is a terminal)
- `--loglevel`: minimum severity of log messages shown (defaults to `ERROR`, so they don't interrupt the report)

Exit codes: `0` if every table is identical, `1` if any differ (including any whose row-level comparison was skipped), and `2` if any comparison itself failed or the run failed as a whole. A table that fails does not stop the others from being compared.

#### Approved task breakdown

Human-readable text-report formatting is deferred until after the JSON report
works; not scheduled as a task yet.

##### Task 0 — Restructure the row-level diff data model

- Split `compare_rows_with_pk`'s single suffixed `mismatched_rows` dataframe
    into separate `mismatched_left`/`mismatched_right` DataFrames, each holding
    the PK columns plus the original (non-suffixed) column names with just
    that side's values — selected off the same shared-PK inner join used
    today, so nothing ever materializes a doubled-width combined frame. This
    also matches the Parquet output shape directly, with no un-suffixing
    needed in Task 4.
- Remove `RowSetDiff.combined` and the module-level `SOURCE_COL` constant —
    nothing consumes the label-tagged concatenation, and it doesn't match the
    separate-left/right-file convention the report and Parquet outputs use.
- Update `KeyedRowDiff` accordingly: `pk_diff: RowSetDiff`,
    `column_mismatches: dict[str, int]`, `mismatched_left`, `mismatched_right`.
- Update all existing unit tests in `diff_test.py` that reference
    `.combined`, `SOURCE_COL`, or `mismatched_rows` to match the new shape.

##### Task 1 — Row-count totals, skip-reason, and a configurable row-level cap

- Add `left_row_count`/`right_row_count` totals to `RowCountDiff` (currently
    it only tracks per-partition mismatches, not the overall totals the report
    needs).
- Add a `row_diff_skipped_reason: str | None` field to `TableDiffResult`,
    populated by `compare_table` with why `row_diff` is `None`: too many rows,
    dtype-incompatible join failure, or mismatched columns (with or without a
    usable primary key).
- Turn `MAX_ROWS_FOR_ROW_LEVEL_COMPARISON` into a `compare_table` parameter
    (`max_rows_for_row_level_comparison: int = MAX_ROWS_FOR_ROW_LEVEL_COMPARISON`)
    so the CLI can override it, keeping the module constant as the default.
- Unit tests: one case per skip reason, row-count totals on partitioned and
    unpartitioned comparisons, and an explicit override of the row-level cap.

##### Task 2 — Timing and peak-memory/CPU instrumentation

- Add `elapsed_seconds: float`, `peak_rss_bytes: int`, and
    `peak_cpu_percent: float` fields to `TableDiffResult`, measured by
    `compare_table` around its own execution.
- Peak RSS and CPU measured via a `psutil`-based background sampler thread
    (polls `psutil.Process().memory_info().rss` and
    `psutil.Process().cpu_percent()` at a short interval, tracks the max of
    each) rather than `resource.getrusage`, avoiding the whole-process/high-
    water-mark and macOS-vs-Linux unit issues; RSS is netted out against a
    pre-call baseline, while CPU percent is a percentage of one core (e.g.
    `400.0` for four cores kept fully busy at once).
- Add `psutil` to `[tool.pixi.dependencies]` in `pyproject.toml`; run
    `pixi install`.
- Unit tests: elapsed time is positive; sampler correctness tested against a
    mocked/fake memory and CPU source rather than relying on real allocation
    timing.

##### Task 3 — Error-tolerant comparison wrapper

- A new outer function (e.g. `run_table_diff`) that wraps `compare_table`,
    catching any exception raised during the comparison and capturing it as an
    error summary (message + traceback) plus a `success: bool`, so a report can
    always be produced even when the comparison itself blows up (e.g. a
    missing datapackage, an S3 access failure) — distinct from the existing
    in-`compare_table` handling of dtype-incompatible row joins, which is an
    expected skip, not an error.
- Unit tests: a simulated failure (e.g. missing datapackage) yields
    `success=False` with a captured error; a normal run yields `success=True`
    with no error.

##### Task 4 — Parquet side-output writer

- A new function, e.g. `write_row_diff_parquet(row_diff, output_path, ...)`,
    that writes `<table_name>_left_only.parquet` / `..._right_only.parquet`
    (schema-matched to the left/right tables respectively) for both the PK
    case (`KeyedRowDiff`: combines `pk_diff.only_in_left`/`only_in_right` with
    `mismatched_left`/`mismatched_right` from Task 0) and the non-PK case
    (`RowSetDiff`: `only_in_left`/`only_in_right` directly).
- Applies the `--max-output-rows` cap, while still recording the
    true total row count (for the JSON report) separately from what was
    written to disk.
- After writing, computes each file's `bytes` (size) and `hash`
    (`"sha256:<hexdigest>"` via `hashlib.sha256`, matching the convention in
    `src/pudl/dagster/assets/core/datapackage.py`) for the report's provenance
    section.
- Unit tests: PK table (symmetric-diff + mismatched rows land on the correct
    side with the original schema), non-PK table, row capping behavior, correct
    `bytes`/`hash` values, and the no-row-diff (skipped) case writing nothing.

##### Task 5 — JSON report structure and serializer

- A new `TableDiffReport` Pydantic model (with nested models for each
    section) matching the detailed schema above, including the provenance
    section (report `created` timestamp; left/right dataset `id`/`created`/
    `git_sha`/`git_tags`, read via a new `PudlDiffDataset.provenance()` method
    that pulls those optional fields from `self.datapackage`) and the Parquet
    output `path`/`bytes`/`hash` fields from Task 4 — with no row-level data
    embedded, only counts and summaries.
- Built from a `run_table_diff` result plus the two datasets' table
    names/paths and provenance.
- `model_dump_json(indent=2)` used for serialization (no separate custom
    serializer needed, unlike the dataclass-based approach originally
    considered).
- Unit tests: report structure for an identical PK table, a differing PK
    table, a differing non-PK table, a schema-mismatch case, a skipped-large-
    table case, an error case, and a dataset with/without git provenance in
    its `datapackage.json` — each checked against the spec's field list.

##### Task 6 — CLI: arguments, orchestration, exit codes

- New `src/pudl/scripts/pudl_diff.py`: required `table_name` argument;
    `--left`/`--right` defaulting to `PudlPaths().parquet_path()` and
    `pudl.PUDL_NIGHTLY_BUILDS_BASE_PATH`; `--right-table`;
    `--output-path` (default cwd, created if missing); `--max-compare-rows`;
    `--max-output-rows`; `--rtol`/`--atol`;
    `--partition-col`/`--no-auto-partition`.
- Orchestrates `run_table_diff` → Parquet side-output writing → JSON report
    serialization → write to `<output-path>/<table_name>_diff.json`. (Superseded
    by Task 9: there is now a single report for the whole run.)
- Exit codes: `0` identical, `1` not identical, `2` error (unknown table,
    dataset load failure not otherwise caught by `run_table_diff`, e.g. bad
    CLI arguments).
- Register `pudl_diff = "pudl.scripts.pudl_diff:main"` in
    `[project.scripts]`.
- Unit tests: end-to-end CliRunner tests against fixture datasets on
    `tmp_path`, covering identical, differing (PK and non-PK), unknown table,
    large-table skip, and a simulated error — checking exit codes and the
    written JSON/Parquet files' contents.

##### Task 7 — Documentation

- Confirm the "The PUDL Diff CLI Tool" section of `pudl-diff.md` reflects
    actual usage now that the tool exists (update if behavior diverged during
    implementation).
- Create a new documentation page `docs/dev/pudl_diff.rst` Explaining
    the purpose of the tool, how to use it, and what the output means.
- Ensure that `pudl_diff --help` provides 2-3 examples of typical usage, including a
    local vs. nightly comparison and a comparison of two local datasets.

##### Task 8 -- Bulk CLI for comparing all tables in two datasets

- Add --color/--no-color option to the CLI for colorized output (default on if stdout is a TTY, off otherwise).

- More precision on the runtime of each table comparison, and the total runtime of the bulk comparison.

- Change --output-path to just --output with a short -o option

- Add a -l and -r short option for the left and right dataset paths, respectively.

- Output a single JSON report summarizing all table comparisons, with a top-level `is_identical` boolean and a list of per-table reports (same structure as the single-table report above). (Done in Task 9, with a dict rather than a list of per-table reports.)

- Need to handle the GeoParquet tables which polars barfs on. (Not addressed by the work in Task 9.)

Done: the `--color/--no-color` option, more precise timings, the `-l`, `-r` and `-o` short options, and comparing all the tables in two datasets in one run.
We kept the long option name `--output-path` rather than renaming it to `--output`.

##### Task 9 — Dataset-level report, and table sizes

Implemented in four commits, so that the move of code could be reviewed separately from the edits to it:

1. Move `_RowSummary` and `_summarize_row_diff` verbatim from the CLI into `pudl.validate.diff`. (`_diff_table` was not moved verbatim, because it returns the CLI-only `_TableOutcome`; it was replaced in commit 3 instead.)
2. Add the dataset-level report to `pudl.validate.diff`, with its tests:
    - `PudlDiffReport`, `PudlDiffSummary` (built by `PudlDiffSummary.from_tables`), `DatasetInfo`, `DiffOptions`, and `build_pudl_diff_report()`; `REPORT_SCHEMA_VERSION` is `1.0.0`
    - `TableDiffReport` loses its dataset-level fields (`created`, `left_dataset`, `right_dataset`)
    - `SizeComparison`, the base class of `TableDiffReport` and `PudlDiffSummary`, with the byte counts and their derived fields; `PudlDiffDataset.table_bytes()`; and `format_bytes()`. `peak_rss_mb` is replaced by `peak_rss`, and `ParquetOutputSummary` gains a `size`.
    - `report_table_diff()`, which runs, writes the Parquet outputs and reports on one table, replacing the CLI's `_diff_table`; and `RowChanges.from_summary()`, which was `_summarize_row_diff`
3. Switch the CLI to the single `pudl_diff_report.json`:
    - The CLI builds one `PudlDiffReport` (also for single-table runs, which are a report with one table) and reads the totals in its summary from it
    - If the run fails before any table is compared, e.g. because the datasets have no tables in common, a report with a top-level `error` is still written, and the exit code is `2`
    - Size columns and summary lines, in green and red
    - `main` split into `_resolve_tables`, `_echo_intro` and `_compare_tables`, to stay under ruff's complexity limit
4. Update `docs/dev/pudl_diff.rst` for the single report.

Remaining before the PR is ready:

- Fully document the report schema
- Add release notes to `docs/release_notes.rst`, with the issue and PR numbers

### Create and organize PUDL Diff subpackage

`src/pudl/validate/diff.py` had grown to ~2,400 lines, and `pudl_diff.py` held a lot
of logic that isn't CLI wiring. The report and comparison code will also be used by a
Marimo notebook and possibly a web app, so we turned it into a subpackage,
`src/pudl/validate/diff/`, and are making the CLI a thin wrapper that parses options
and dispatches.

#### Layout

The code now lives in these modules (Phases 1–3, done).

Modules in `src/pudl/validate/diff/`:

| Module              | Contents                                                                                                                                                                                                                                 |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `__init__.py`       | Overview of how to run a comparison and how the modules are layered                                                                                                                                                                      |
| `base.py`           | `ReportModel`, the base class of the report's Pydantic models: it makes each field's docstring its description in the JSON Schema                                                                                                        |
| `formatting.py`     | `format_bytes`, `format_elapsed`, `format_duration`, `format_percent`, `format_signed_percent`                                                                                                                                           |
| `dataset.py`        | `DatasetProvenance`, `PudlDiffDataset`, `resolve_tables`, `NoTablesError`                                                                                                                                                                |
| `schema.py`         | `SchemaDiff`, `compare_schemas`                                                                                                                                                                                                          |
| `row_counts.py`     | `RowCountDiff`, `NO_PARTITION`, `compare_row_counts`, `count_rows`, dbt partition-expr helpers                                                                                                                                           |
| `rows.py`           | `RowSetDiff`, `KeyedRowDiff`, hashing/spill, `compare_rows_with_pk`, `compare_rows_without_pk`                                                                                                                                           |
| `performance.py`    | `PerformanceSampler`                                                                                                                                                                                                                     |
| `table.py`          | `TableDiffResult`, `compare_table`, `TableDiffRun`, `run_table_diff`, `row_diff_left_right_frames`, `MAX_ROWS_FOR_ROW_LEVEL_COMPARISON`, `RowComparisonSkipReason`                                                                       |
| `outputs.py`        | `ParquetOutput`, `RowDiffParquetOutputs`, `write_row_diff_parquet`                                                                                                                                                                       |
| `table_report.py`   | `*Summary` classes, `RowChanges`, `SizeComparison`, `TableDiffReport`, `DiffOptions`, `report_table_diff`, `RowDiffSectionSkipReason`                                                                                                    |
| `dataset_report.py` | `DatasetInfo`, `PudlDiffSummary`, `PudlDiffReport`, `build_pudl_diff_report`, `REPORT_SCHEMA_VERSION`, `TableOutcome`, `table_outcome`                                                                                                   |
| `report_schema.py`  | `report_json_schema` (the JSON Schema of the report, generated from its models), the committed copy at `docs/_static/pudl_diff_report.schema.json` and its `main`, and the helpers that flatten the schema into models for the docs page |
| `runner.py`         | `run_dataset_diff`: resolves the tables, compares each, and returns the `PudlDiffReport`, reporting progress through callbacks                                                                                                           |
| `terminal.py`       | Text rendering of reports for a terminal: the styling constants, per-table lines, end-of-run summary, and `TerminalProgress`, which supplies `run_dataset_diff`'s callbacks                                                              |

Modules only import from those above them in this order (no cycles; checked with an
`ast` scan before starting):

```text
formatting, dataset, performance
  → schema, row_counts, rows
  → table → outputs
  → table_report → dataset_report
  → runner, terminal
```

`runner` and `terminal` don't import each other: `runner` reports progress through
callbacks, and `terminal.TerminalProgress` supplies them for the CLI.

- `rows.py` imports `count_rows` from `row_counts.py`.
- `outputs.py` comes after `table.py` because it uses `row_diff_left_right_frames`.
- `dataset_report.py` imports `DiffOptions`, `RowChanges`, `SizeComparison` and
    `TableDiffReport` from `table_report.py`.
- Module-level constants and type aliases live with the code that uses them:
    `NO_PARTITION`, `_PARTITION_COL_NAME`, `_EXTRACT_YEAR_RE` and `_BARE_COLUMN_RE` in
    `row_counts.py`; `_ROW_KEY_COL` and the other row-key column names in `rows.py`;
    `MAX_ROWS_FOR_ROW_LEVEL_COMPARISON` and `RowComparisonSkipReason` in `table.py`;
    `RowDiffSectionSkipReason` in `table_report.py`; `REPORT_SCHEMA_VERSION` in
    `dataset_report.py`.

Other consumers (a notebook, a web app) need only `runner.run_dataset_diff(...) -> PudlDiffReport` and the `*_report` modules, which are Pydantic models, so
`model_dump_json()` / `model_validate_json()` handle the JSON. They never touch
`terminal.py`. `pudl_diff.py` keeps only the Click options, `_set_log_level`, wiring
`terminal.TerminalProgress` in as the progress callbacks, writing the report file, and
the exit code. It is now ~260 lines, down from ~780.

The unit tests mirror the layout: one `*_test.py` per library module in
`tests/unit/validate/diff/`. The helpers that build datasets to compare are fixtures in
`tests/unit/conftest.py` (`write_datapackage`, `pk_resource`, `no_pk_resource`,
`make_dataset` and `write_two_datasets`), so that the CLI tests in
`tests/unit/scripts/pudl_diff_test.py`, which keep only the tests that invoke the CLI,
share them rather than having their own copies.

#### Commit sequence

<!-- The steps below are numbered continuously across the phases, and are referred
to by number, so markdownlint's ordered list prefix rule is off for them. -->

<!-- markdownlint-disable MD029 -->

Following the repo conventions, code is moved verbatim first; edits come in separate
commits, and need explicit approval. Every commit passed the unit tests for the diff
code (191 before the tests were extended, 220 at the end), `ruff`, `pyrefly-check` and
the pre-commit hooks. `tests/pipeline/validate/pudl_diff_test.py`
can't be run interactively, so only its imports were updated.

**Phase 0 — Preflight** (done; no commit)

- Dependency scan (no cycles), and a baseline of tests, `pyrefly-check` and `ruff`.
- The scan skipped uppercase names, so it missed the constant `NO_PARTITION`; it was
    found when extracting `row_counts.py` and moved there.

**Phase 1 — Verbatim moves, imports only** (done)

Each commit updated every importer (the CLI and all three test files) in the same
commit. The only edits besides the moves were imports and a docstring on each new
module.

1. `git mv validate/diff.py validate/diff/table_report.py`, add `__init__.py`.
2. Extract `formatting.py` (`format_bytes`).
3. Extract `dataset.py` (`DatasetProvenance`, `PudlDiffDataset`).
4. Extract `schema.py`.
5. Extract `row_counts.py`, with the partition-expression helpers.
6. Extract `rows.py`.
7. Extract `performance.py`.
8. Extract `table.py`, with `_row_diff_left_right_frames`. The two tests that patch
    `compare_rows_with_pk` and `get_dbt_partition_expr` now patch them in the `table`
    module, where `compare_table` looks them up.
9. Extract `outputs.py`.
10. Extract `dataset_report.py`. What remained is `table_report.py`, which the CLI and
    tests now import by that name (there was a temporary `diff` alias while carving).
    A test local variable that shadowed the module name was renamed
    `table_diff_report`.

**Phase 2 — Move CLI code into the library, still verbatim** (done)

11. `_format_elapsed`, `_format_duration`, `_format_percent` and
    `_format_signed_percent` into `formatting.py`.
12. The rendering code (`_render*`, `_change_segments`, `_row_cells`, `_format_header`,
    `_format_outcome`, `_echo_*`, and the styling constants) into the new
    `terminal.py`. This was done *after* step 13, since it depends on `_TableOutcome`.
13. `_TableOutcome` and `_outcome` into `dataset_report.py`.
14. `_resolve_tables` and `_NoTablesError` into `dataset.py`.
15. `_compare_tables` into the new `runner.py`, still printing directly, and so still
    importing `terminal`.

**Phase 3 — Edits** (done, except step 20)

16. Dropped the underscore from names now imported across modules: `format_elapsed`,
    `format_duration`, `format_percent`, `format_signed_percent`, `resolve_tables`,
    `NoTablesError`, `TableOutcome`, `table_outcome` (was `_outcome`),
    `PerformanceSampler`, `count_rows`, `row_diff_left_right_frames`, `echo_intro`,
    `echo_summary`, `format_header` and `format_outcome`. Names used within one module,
    such as the terminal styling constants, stay private, as does the CLI's
    `_set_log_level`. Ruff requires docstrings on public names, so
    `PerformanceSampler`'s `__init__`, `__enter__` and `__exit__` and `count_rows` got
    one-line docstrings. Also tidied the redundant imports in `dataset_report.py`.
17. Replaced `_compare_tables` with `runner.run_dataset_diff`, which returns a
    `PudlDiffReport` (with `error` set if there was nothing to compare, rather than
    raising) and reports progress through optional `on_tables_resolved` and
    `on_table_compared` callbacks. `terminal.TerminalProgress` supplies them for the
    CLI and keeps each table's `TableOutcome` for the summary. `runner.py` no longer
    imports `click` or `terminal`. The report's elapsed time now starts when the
    comparison does, so it no longer includes setting up the datasets and options.
    Added unit tests for `run_dataset_diff` and `TerminalProgress`. (This commit
    initially introduced four pyrefly errors, which were fixed before moving on.)
18. Added tests that a `PudlDiffReport` round-trips through `model_dump_json()` /
    `model_validate_json()`, both for a comparison that found changes and for one that
    failed. `RowChanges` is not part of the serialized report: `PudlDiffSummary. from_tables` uses it only as a local intermediate, and `TableOutcome` holds it for
    display. It stays a frozen dataclass.
19. Wrote the `__init__.py` overview and a new `table_report.py` docstring, pointed
    `docs/dev/pudl_diff.rst` at `run_dataset_diff`, and changed docstring roles that
    named something that moved to another module to the `:class:`~.Name\`\` form, which
    Sphinx resolves by suffix. `docs-check` passes with no warnings (it isn't
    nitpicky, so the suffix references have not been checked to resolve). Split the
    tests to mirror the layout, as described above, and moved the terminal and
    formatting tests out of the CLI tests. Shared test code has to be fixtures in a
    `conftest.py`: a `helpers.py` isn't possible, since the `name-tests-test` hook only
    allows `*_test.py` and `conftest.py`, and pyrefly can't resolve `from tests...`
    imports. The fixtures started out in `tests/unit/validate/diff/conftest.py`, and
    moved up to `tests/unit/conftest.py` so that the CLI tests could use them too.
20. Add a release notes entry to `docs/release_notes.rst`, with the issue and PR
    numbers (still to do: there is no PR for the branch yet).

<!-- markdownlint-enable MD029 -->

##### Row and column order

The comparisons are meant to be insensitive to the order of a table's rows and
columns. Before this work, only `compare_schemas` (column order) and a two-row primary
key case (row order) were tested. Added tests that shuffle both, for tables with and
without a primary key (including a composite one, in either key order), for the row
comparisons and for `compare_table`, plus a test that real changes are still found in
a table whose rows and columns are in a different order.

### Nightly PUDL Data Diff Reporting

We want to make `pudl-diff` an integral part of the nightly PUDL data build so we can easily identify unexpected changes, and provide a transparent public interface for users to see what has changed between builds and between stable releases.

- Define a new Dagster asset that conditionally run s`pudl-diff` against a given baseline at the end of the PUDL ETL pipeline.
- Its only upstream dependency will be the `pudl_datapackage` asset, since `pudl-diff` reads build provenance information from the `datapackage.json` descriptor.
- We don't want to run `pudl-diff` on *every* build, since in local development it's not always clear what we ought to be comparing against, and downloading a whole baseline dataset from S3 can take a long time.
- By default, we want to materialize the `pudl-diff` report any time we are running the full ETL using Google Batch.
- The baseline (left dataset) or baselines that we compare against will depend on what kind of build we are doing.
    - If we are doing a `branch` build, we will compare against the last successful nightly build, which is stored in S3 at `s3://pudl.catalyst.coop/nightly/` (and is the default baseline for the CLI).
    - If we are doing a scheduled `nightly` build that was triggered by our GitHub Actions workflow, we will compare against the last successful nightly build, which is stored in S3 at `s3://pudl.catalyst.coop/nightly/`. We will **also** compare against the last successful versioned release so that we can be aware of all the changes which have accumulated in the pipeline outputs since that release. The last successful versioned release is stored in S3 at `s3://pudl.catalyst.coop/stable/`. This means we will need to output two distinct `pudl-diff` reports.
    - If we are doing a stable versioned release build (i.e. with a version tag like `v2026.10.0`) then we will only compare against the last successful versioned release, which is stored in S3 at `s3://pudl.catalyst.coop/stable/`. This means we will only output one `pudl-diff` report.
- For *all* build types we will want to save the `pudl-diff` reports to the builds bucket along with the other build outputs, under `gs://builds.catalyst.coop/<build-id>`
- For scheduled `nightly` and versioned `stable` releases, we will also want to deploy the `pudl-diff` reports to our public S3 and GCS buckets so that users can see what has changed between builds and between stable releases. The public S3 bucket is `s3://pudl.catalyst.coop/nightly/` for nightly builds and `s3://pudl.catalyst.coop/stable/` for stable releases. The public GCS bucket is `gs://pudl.catalyst.coop/nightly/` for nightly builds and `gs://pudl.catalyst.coop/stable/` for stable releases.
- In local development, by default we will not materialize the `pudl-diff` report as part of the Dagster ETL, but we want to allow the user to override this behavior intentionally, and materialize the report by setting an environment variable like `PUDL_DIFF_RUN=true` or maybe also by setting a Dagster config option like `pudl_diff_run: true`. If the user as set this override, then by default the `pudl-diff` asset will use `PUDL_NIGHTLY_BUILDS_BASE_PATH` as the baseline for comparison, but we should allow the user to override that by setting an environment variable like `PUDL_DIFF_BASELINE_PATH` to point `pudl-diff` at a pre-existing local reference dataset for comparison.
- Because there are cases in which we will want to output more than one `pudl-diff` report from the same build, we'll need to come up with a naming convention for the `--output-path` directory. The name should clearly indicate which two build outputs were being compared. For complete builds with up-to-date `datapackage.json` descriptors we could use the `id` field which is currently a UUID. This would give an output path like `left-uuid-vs-right-uuid` which would be unique and unambiguous, but not very human-readable.
- To improve readability while maintaining uniqueness, we can include the git tag (if present) of the builds being compared. For example:
    - `nightly-2026-09-15-uuid-vs-nightly-2026-09-16-uuid` for a current nightly build comparison against the previous nightly build.
    - `nightly-2026-09-15-uuid-vs-branch-2026-10-31-1939-3e5887d46-my-branch-name-uuid` for a comparison between a branch build (using the branch build ID as the prefix) and the previous successful nightly build.
    - `v2026.9.0-uuid-vs-nightly-2026-09-16-uuid` for a comparison between the current nightly build and the previous stable release.
    - `v2026.9.0-uuid-vs-v2026.10.0-uuid` for a comparison between the current stable release and the previous stable release.

#### Approved design and decisions

- Planning (`src/pudl/deploy/pudl_diff.py`): branch and nightly builds compare against
    both `nightly/` and `stable/` in the `pudl_diff` Dagster asset (depends only on
    `pudl_datapackage`; enabled by `dg_nightly.yml`, or locally by `PUDL_DIFF_RUN=true` /
    `pudl_diff: {run: true}` config, with `PUDL_DIFF_LEFT_ROOT` overriding the baseline).
    Baselines are read from public S3 (anonymous, free egress); the public GCS bucket is
    requester-pays, which Polars can't read.
- Stable-tag builds make no report in the ETL: a stable deploy may reuse a nightly
    build's outputs, and a report against ephemeral paths would break the release-to-
    release provenance. `pudl_deploy` instead diffs the prepared outputs against the
    previous stable release (found by listing `vX.Y.Z` prefixes in the public bucket)
    and records the permanent versioned paths as the dataset roots, using the new
    `PudlDiffDataset(display_root=...)`. This fails the deploy if it can't be made;
    reports carried in the build outputs are replaced.
- Directory names are `<left git tag>-vs-<right git tag or build ID>`. Branch builds
    have no tag, so use the build ID; a baseline with no tag falls back to a short `id`.
- Reports live in `$PUDL_OUTPUT/pudl_diff/`, so they reach the builds bucket and the
    public buckets with the other outputs. They are excluded from the `eel-hole` path,
    and archived as `pudl_diff.zip`, which the Zenodo release picks up (it ignores
    subdirectories).
- Not yet addressed: EPA CEMS diff peak RSS (~64 GB) vs the deploy VM's 32 GB.

### Preparing to extract the tool into its own package

We are going to move the tool out of the PUDL repository into its own independently
installable package, so that it is useful beyond PUDL, doesn't add 8,000 lines to
PUDL's review, and can be installed by scheduled jobs that archive every nightly and
stable report. In this branch we first make `pudl.validate.diff` independent of PUDL:

- Dropped the dbt row-count partitioning entirely (`--partition-expr`,
    `--no-auto-partition`, the partition report fields): it was brittle, tied the tool to
    PUDL's repo layout, and was noisy. The left-only and right-only Parquet outputs show
    what is behind a row-count change. The report schema version stays `1.0.0`, since no
    report has been published.
- Standard library logging under `pudl_diff.*` loggers. PUDL's `configure_root_logger`
    also configures the `pudl_diff` logger.
- `defaults.py` is the only module that refers to PUDL, lazily and optionally: the
    nightly root, the local outputs directory, and a primary-key fallback chain (the
    dataset's own datapackage, then `PUDL_PACKAGE` if importable, then the last nightly
    build's datapackage). A table with no primary key found anywhere is treated as
    keyless with a warning. A unit test keeps every other module free of PUDL imports.
- The report's JSON Schema lives beside the code, and the docs build copies it to
    `_static`. The diff test fixtures and CLI tests moved into `tests/unit/validate/diff`.

Extraction itself: `git filter-repo` with the path list and `--path-rename`, rename the
package to `pudl_diff` in a separate commit, scaffold from the Catalyst template, rewrite
bare `#123` references to `catalyst-cooperative/pudl#123`, then integrate into PUDL on a
fresh branch holding only the glue (deploy planning, asset, `dg_nightly.yml`).

### The PUDL Diff Marimo Notebook

Now that we have a stable, structured metadata report for each PUDL Diff output, and a
clean, performant way to store the actual difference between two PUDL parquet datasets,
it's time to build some tooling that will help us understand what's going on visually.

Humans are visual creatures, and good visualizations (with accompanying data to back
them up) make it much easier for us to parse large amounts of data quickly. This will
help us understand and improve the open energy system data that PUDL produces, and
ensure that it's of the highest quality possible.

Marimo computational notebooks are a great way to build and share interactive data
visualizations, and they are well integrated with coding agents. They can be run as
scripts and exported to a variety of formats. They can also often run in-browser, and we
are already hosting a number of Notebooks as PUDL examples, so it's a natural fit to
build our PUDL Diff visualizations using Marimo Notebooks

#### Goals of the PUDL Diff Marimo Notebook

##### Notebook Motivations

- Make it easy for developers and data users to understand how the PUDL datasets are evolvoing over time.
- Make it easy to quickly identify unexpected changes in the PUDL datasets, and trace them back to their source.
- Provide transparency into our data QA/QC processes for the public and our open data users.
- The JSON report, summary, and terminal output can provide reasonable insight into the dataset and table level differences between two sets of PUDL outputs, but they can't easily provide compact column and row level insights.
- The PUDL Diff Marimo notebook will provide visual summaries that correspond to the PUDL Diff Summary and the full terminal output that summarizes changes at the table level which are analogous to what the current CLI can do.
- Additionally, the PUDL Diff Marimo notebook will allow the user to select individual columns within a table and visualize how the contents of that column differ between datasets.
- For the most detailed level of data debugging, users will also be able to load the Parquet outputs of the PUDL Diff tool and visualize the actual rows that differ between datasets interactively.
- If it's easy to do, we should also make it possible to query and load the full original parquet tables that are referred to as being the left and right datasets in the PUDL Diff report. This won't always be possible because the nightly build outputs are ephemeral, but for the most recent nightly build, and for stable releases within the last year or two, it should be possible. Outputs from all the nightly and branch builds from the last 30 days will also be available to Catalyst Cooperative members in the private builds.catalyst.coop bucket for debugging.

##### Notebook Legibility

- The notebook needs to tell a story about the data that's immediately legible visually, and help the user understand what has changed, and whether that might be a problem or be expected.
- The notebook should be easy to read and understand, and should be visually compelling. It should also be easy to navigate, with clear headings and sections, that automatically generate a table of contents via the Marimo notebook infrastructure that allows the user to jump to different sections of the notebook.
- The notebook should provide links and references that give the data additional context and provenance. For example a link to the PUDL repository at the git commit that produced the left and right datasets, a link to the underlying PUDL Diff JSON & Parquet report for the comparison, a link to the PUDL documentation for the datasets and tables being compared, and a link to the PUDL Diff documentation for the report schema and the meaning of the various fields in the report.

#### Implementation Choices

##### Where to implement

- In general, we will want to implement the analysis required to make a visualization in a library module, not in the notebook itself. It is easier to do code review, testing, debugging, and development in modules, and also makes the code more reusable.
- We will also probably want to implement some reusable visualization components in a library module, so that we can use them in multiple notebooks and other applications. This will also make it easier to test the visualizations and ensure that they are working correctly, and avoid wasteful duplication of code.
- The notebook interface itself is mostly for presentation, publication, and interaction.

##### Technology choices

- We are using Marimo notebooks for the interface.
- We are focused on 2-dimensional visualizations.
- Where it is helpful, we want to enable users to interact with the visualiztaions, but we also don't want them to be overwhelmed, and we don't want to hide the interesting insights that the data holds by requiring users to set all the knobs and sliders to exactly the right (but invisible) values.
- We are open to using any python based visualization library that is well maintained, documented, and integrates cleanly with Marimo notebooks. We want the visualizations to be beautiful, compelling, performant, and easy to understand.
- You should provide a summary of the visualization library options, with the pros and cons of each, and a recommendation for which library to use for the PUDL Diff Marimo notebook.
- Initially these notebooks will be running locally on the user's machine, which will probably be MacOS or Linux. They will have plenty of memory and probably work with local data a lot. However, eventually we want to host these notebooks online somewhere, and may want them to be able to run in-browser without the user needing to manage the data or a python environment, via WASM.

##### Querying the PUDL Diff Report

- The data sitting behind the PUDL Diff report is all stored in Apache Parquet files. This includes both the "left-only" and "right-only" subsets that are stored as part of the report, and the original full tables that are being compared (though they may be remotely stored, or no longer available at the time of visualizations).
- Given that some of the tables are quite large, we should be careful about how we load and query the data, and use tools that are designed to deal with larger datasets. Both Polars and DuckDB are good options for doing the querying. We will need to figure out which one we want to work with. We might also use a hybrid, with dedicated SQL cells in the notebook that use DuckDB to query the data, and then load the results into Polars for further analysis and visualization interactively in the notebook, since most of us are more familiar with dataframes than SQL.
- The structured JSON version of the report is pretty small, and many different tools can be used to read it. Pydantic will probably be used to load and validate it, and then the resulting model can be handed off however is most convenient for use by the rest of the functionality in the notebook.

#### Visualizing the PUDL Diff Summary Report

- At the top of the notebook, I want to have a summary of the whole PUDL Diff report.
- Conceptually this will serve the same purpose as the high level summary that's generated by the CLI, but it will be primarily visual, instead of textual.
- For indidivual headline numbers we can have a big number with a label.
- For the row, column and schema summaries we can have a small bar chart or other visual representation of the number of added / removed / changed rows, columns and schema elements.
- This section will probably be the least interactive sine the data is relatively shallow, but it will also be the first thing that the user sees when they open the notebook, so it should be visually compelling and easy to understand.

#### Visualizing Table-Level Diffs

- Show not just *that* the table, changed, but **how** it changed.
- For each table, show a visual and textual representation of the number and percentage of rows that were added, removed, or changed, as well as the number that were unchanged.
- Note that many of our tables have thousands to many millions of rows. A handful have more than a billion rows. This will require some thought about how to visualize the data in a way that is legible, informative, and also performant. The diffs will generally be only a small fraction of the size of the full tables, but can still be very substantial.
- We have a mix of tables that have primary keys and those that don't, and the visualizations should be different for each case.
- There's a lot more we can show about tables with primary keys, because we know which rows are supposed to correspond to each other across the two datasets. Whereas with the tables that lack primary keys we can only say that the contents of the table changed, but we can't say which rows correspond to each other.
- A visual and textual representation of the **schema** changes should also be presented. This will include the number of columns that were added, removed, or changed, as well as the number of columns that were unchanged, and in the case of columns whose schema has changed, a visual representation of the nature of the change (e.g. data type change, nullability change, membership in the primary key, etc.).
- For each **column** in the table, show numerical and visual representations of the number and percentage of **rows** that were added, removed, or changed, in that column as well as the number that were unchanged. This will help the user understand if there is a single mechanism that's responsible for change across all of the changed columns, or if there are multiple mechanisms that are responsible for the changes in the table, each affecting a different subset of the columns.
- It would be nice to provide a "minimap" style visualization of the table, where each column is represented as a narrow vertical arrangement of points, where the color of each point represents how a corresponding row changed (green: added, yellow: changed, red: removed, gray: unchanged), and the position of the point along the line indicates **which** row changed. This will allow the user to quickly see which columns were most affected by changes between datasets, whether the changes across different columns are happening in the same or different rows, and at a coarse level, what kind of change is happening add, content change, removal, or no change). I imagine this visualization looking reminiscent of a genetic sequence alignment, where each column corresponds to the genetic sequence of a different organism, and the rows are individual genes, base-pairs, or highly conserved sequences, and the color of each point might indicate which allele of a given gene the organism has, or whether a sequence has been added or removed between individuals. In our case, the columns correspond to the different columns in the table, and the rows are the different rows in the table, and the color of each point indicates whether that row was added, removed, changed, or unchanged between datasets. This kind of visualization will probably only be possible for tables with primary keys, since we need to know which rows correspond to each other across datasets in order to visualize the changes in this way.

#### Visualizing Column-Level Diffs

- Within a specific table, the use should be able to select a column that they want to drill down into visually.
- For different kinds of selected columns the provided visualizations will be different.
- For all 2-dimensional visualizations, we can use the left vs. right datasets as the two dimensions. Left is always take as the reference dataset, and right is the changed dataset. This means that left should always be on the x-axis (independent variable), and right should always be on the y-axis (dependent variable).

##### Numerical columns

- For numerical columns, we can visualize the distribution of values in the left and right datasets.
- We can use a scatter plot or a 2-dimensional histogram or heat map to visualize the distribution of values in the left and right datasets, with the x and y axes scaled to the numerical range of the column.
- We can also show projected 1-dimensional histograms of the left and right datasets along the x and y axes, respectively, to show the distribution of values in each dataset individually.
- In the case of a column that experienced no changes, we would expect to see a diagonal line of points from the bottom left to the top right of the plot, indicating that the values in the left and right datasets are identical. In the case of a column that experienced changes, we would expect to see points scattered away from the diagonal line, indicating that the values in the left and right datasets are different.
- There are at least two different ways that we can use color in the context of this visualization. Color can indicate the density of points in a given area of the plot, or it can indicate whether a given point is an added, removed, or changed data point. We should probably provide both options to the user, and allow them to toggle between them. On plots with a small number of points, we might also be able to use the plotting symbol instead of color to indicate whether a given point was added, removed, or changed, but many of our plots will have thousands or millions of points, so the symbols will be too cluttered.
- Another option here is to use **color** to indicate the **kind** of change, and **opacity** (or alpha) to indicate the **magnitude** of the change. For example, a point that was added might be colored green, a point that was removed might be colored red, and a point that was changed might be colored yellow, but if all of the points are semitransparent, then the density of points in a given area will be indicated by the opacity of the color. This will allow us to visualize both the kind and magnitude of changes in a single plot.
- There may be other useful ways to combine color, opacity, symbols, and point location to visualize the changes in a numerical column. Feel free to provide examples of other ways to visualize the changes in a numerical column, and we can discuss them and decide which ones to implement.

##### Categorical columns

- We can treat categorical columns similarly to numerical columns, but in the case of categorical columns the axes don't represent numerical values, and instead the unique Focategorical values themselves.
- We can have colored boxes (heatmap/matrix style) representing different combinations of left and right values, and we can label the axes to indicate the unique value that that x or y position pertains to in the plot.
- I think the value that we want to use at each of these intersection points is the ratio of the number of occurrences of that value in the left dataset in that column, to the number of occurrences of that value in the right dataset in that column. If the two datasets were identical in that column then the diagonal cells would all have a value of 1.0, and the off-diagonal cells would all be 0/0 which we can represent as NA or 0.0. If a value is more common in the left dataset than the right dataset, then the ratio will be greater than 1.0, and if a value is more common in the right dataset than the left dataset, then the ratio will be less than 1.0. The two diagonal halves of the plot will represent the same information, so we might want to only show half of the matrix to avoid duplication. This will allow us to see which how the frequency of a given value changes between the left and right dataset for a given column. Alternatively, the color could correspond to the delta between the number of occurrences of a given value in the left and right datasets, or the percentage change between the two datasets. We should explore different options and see which ones work best.
- For larger numbers of unique values, labeling all of the individual unique values on the x and y axes will become hard to read, and a
    pure heat map style diagram is not as useful in the categorical value case as it is for numerical values, because without the per-category axis labels, the axes don't have a lot of semantic meaning. However, we can work around this if there is any kind of hierarchical structure in the categorical values. For example, if the categories represent fuel types, we might have a set of categories which all represent coal (anthracite, bituminous, subbituminous, lignite...) and if they were all put next to each other and visually separated from the other higher level categories (e.g. liquid fuels, gaseous fuels) then the matrix of values would still convey visual information.

#### Presenting Row-Level Diffs

- The most detailed view of the data is the row-level diffs. These will be presented as actual dataframes (or interactive tables derived from dataframes).
- The table of values should allow the user to sort and filter the rows, and should provide visual summaries and potentially numerical summaries of each column in association with the table to help the user understand what's going on.
- Given that we have a left-only and a right-only set of rows for each table, we can present the left-only rows in one table, and the right-only rows in another table. They should be on the left and right sides, respectively.
- We can also provide a way to join the two tables together on the primary key (if it exists) so that the user can see the left and right values for each row side by side, potentially with just a subset of columns selected. The left and right versions of a given column should be displayed adjacent to each other for easy comparison.
- The user will also want to be able to do their own selection, manipulation, and plotting of the left-only, right-only, and potentially merged tables, probably using Polars or Pandas interactively.
- We might also want to experiment with styling the tabular data display to highlight the differences between the left and right values in a given column, for example by coloring the background of the cells that have changed, or coloring the background with a colormap that's based on the numerical value stored in the cell, or by using a combination of both. We should experiment with different styling options and see which ones work best.
