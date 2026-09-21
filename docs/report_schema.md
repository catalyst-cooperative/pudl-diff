# PUDL Diff Report Schema

`pudl_diff` writes a JSON report of its comparison, described in [Usage](usage.md).
This page is the reference for every field of that report. It is generated from the
report's Pydantic models, which are also where the descriptions below are written, so
it always matches the code.

The report is described by a [JSON Schema](https://json-schema.org), which you can
use to validate a report, or to give a program or a coding agent a machine-readable
description of it:
[pudl_diff_report.schema.json](_static/pudl_diff_report.schema.json).
The same schema is available in Python, and the report can be loaded and checked with
Pydantic:

```python
from pudl_diff.dataset_report import PudlDiffReport
from pudl_diff.report_schema import report_json_schema

schema = report_json_schema()
report = PudlDiffReport.model_validate_json(report_path.read_text())
```

Every field listed here is always present in a report, even if its value is `null`.
A field marked *derived* is calculated from the others: it is written to the report
for convenience, but ignored when a report is loaded. The version of the report format
is in its `schema_version` field, which follows the `major.minor.patch` convention.

The copies of the schema and of this page in the repository are kept up to date by the
`pudl-diff-schema` pre-commit hook, which rewrites them when the code in `pudl_diff`
changes, and checked by a unit test. To update them by hand, run
`pixi run python -m pudl_diff.report_schema`.

## PudlDiffReport

The full comparison of two PUDL datasets: the saved JSON report.

Built by `build_pudl_diff_report()`. Holds everything that pertains to the comparison as a whole, plus a `TableDiffReport` for each table.

### `schema_version`

*Type:* string.

Version of this report format, in `major.minor.patch` form.

### `created`

*Type:* string.

UTC ISO-8601 timestamp of when this report was generated.

### `elapsed_seconds`

*Type:* number or null.

Wall-clock time the whole comparison took.

### `left_dataset`

*Type:* [DatasetInfo](#datasetinfo).

The reference dataset, e.g. the last nightly build. Additions, removals and changes are all measured from it to the right dataset.

### `right_dataset`

*Type:* [DatasetInfo](#datasetinfo).

The dataset that was compared against the left one, e.g. a local build.

### `options`

*Type:* [DiffOptions](#diffoptions).

The settings the comparison was run with.

### `tables_only_in_left`

*Type:* array of string.

Tables found only in the left dataset, which aren't compared.

### `tables_only_in_right`

*Type:* array of string.

Tables found only in the right dataset, which aren't compared.

### `summary`

*Type:* [PudlDiffSummary](#pudldiffsummary).

Totals over all the compared tables.

### `tables`

*Type:* object mapping names to [TableDiffReport](#tablediffreport).

Each compared table's report, keyed by its name in the left dataset.

### `error`

*Type:* string or null.

Why the comparison as a whole failed, e.g. no tables could be listed. This is `null` when the only failures are of individual tables, which each record their own `error`.

### `success`

*Type:* boolean. *Derived from the other fields.*

Whether the comparison completed.

That is, `error` is `null` and so is every table's. Distinct from `is_identical`: a comparison can succeed and still find differences.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether every compared table is identical, and `success` is `true`.

Tables found in only one dataset don't count against this.

## DatasetInfo

One of the two compared datasets: where it is, and where it came from.

### `id`

*Type:* string or null.

The dataset's build UUID.

### `created`

*Type:* string or null.

UTC ISO-8601 timestamp of when this dataset was built - distinct from the report's own `created`, which is when the *comparison* was run.

### `git_sha`

*Type:* string or null.

The git commit SHA of the PUDL code that built the dataset.

### `git_tags`

*Type:* array of string or null.

The git tags on that commit, e.g. release versions like `v2026.1.0`.

### `root`

*Type:* string.

The root path or URL of the dataset's Parquet files. For a dataset on the local filesystem, an absolute path with any symlinks resolved, so it doesn't depend on the directory the comparison was run from.

## DiffOptions

The settings a dataset comparison was run with.

Recorded in the report because they affect how its results should be interpreted, e.g. whether a table's row-level comparison was skipped.

### `rtol`

*Type:* number.

Relative tolerance for float equality, as in `numpy.isclose()`.

### `atol`

*Type:* number.

Absolute tolerance for float equality, as in `numpy.isclose()`.

### `max_compare_rows`

*Type:* integer.

Row-level comparison is skipped for any table with more rows than this.

### `max_output_rows`

*Type:* integer or null.

Cap on the rows written to each Parquet side-output file, or `null` to write every differing row.

## PudlDiffSummary

Totals over every table in a `PudlDiffReport`.

Saves consumers from aggregating the tables themselves.

The size fields (see `SizeComparison`) total only the tables whose size is known on both sides.

### `left_table_bytes`

*Type:* integer or null.

Size in bytes of the left table's Parquet file(s), or `null` if unknown.

### `right_table_bytes`

*Type:* integer or null.

Size in bytes of the right table's Parquet file(s), or `null` if unknown.

### `table_count`

*Type:* integer.

Number of tables compared, including any whose comparison failed.

### `identical_table_count`

*Type:* integer.

Tables whose comparison completed and found no differences.

### `changed_table_count`

*Type:* integer.

Tables whose comparison completed and found differences.

### `failed_table_count`

*Type:* integer.

Tables whose comparison failed to complete.

### `failed_tables`

*Type:* array of string.

The names of the tables whose comparison failed.

### `schema_changed_tables`

*Type:* array of string.

Tables with columns added or removed, or with changed dtypes.

### `left_row_count`

*Type:* integer.

Total rows in the left tables, over all tables that could be counted.

### `right_row_count`

*Type:* integer.

Total rows in the right tables, over all tables that could be counted.

### `rows_added`

*Type:* integer.

Rows only in the right table, summed over tables with a row-level comparison.

### `rows_changed`

*Type:* integer.

Rows with the same primary key but changed values, summed over tables with a row-level comparison and a primary key.

### `rows_removed`

*Type:* integer.

Rows only in the left table, summed over tables with a row-level comparison.

### `no_row_diff_table_count`

*Type:* integer.

Tables with no row-level comparison, whether skipped or failed. Their rows count towards the row totals, but not the rows added, changed or removed.

### `no_row_diff_left_row_count`

*Type:* integer.

Total rows in the left side of those tables.

### `columns_added`

*Type:* integer.

Columns only in the right table, summed over all the tables.

### `columns_changed`

*Type:* integer.

Shared columns whose dtype changed, summed over all the tables.

### `columns_removed`

*Type:* integer.

Columns only in the left table, summed over all the tables.

### `peak_rss_bytes`

*Type:* integer or null.

The highest `peak_rss_bytes` of any table.

### `peak_rss_table`

*Type:* string or null.

The table with that peak memory use.

### `left_table_size`

*Type:* string or null. *Derived from the other fields.*

`left_table_bytes` in human-readable form, e.g. `12.3 MB`.

### `right_table_size`

*Type:* string or null. *Derived from the other fields.*

`right_table_bytes` in human-readable form.

### `bytes_difference`

*Type:* integer or null. *Derived from the other fields.*

The change in size from the left to the right table.

`right_table_bytes - left_table_bytes`, so negative if the right side is smaller. Compression changes show up here even if the contents don't.

### `bytes_difference_size`

*Type:* string or null. *Derived from the other fields.*

`bytes_difference` in human-readable form, e.g. `-1.2 MB`.

### `bytes_difference_percent`

*Type:* number or null. *Derived from the other fields.*

`bytes_difference` as a percentage of `left_table_bytes`.

`null` if the left size is unknown or zero.

### `peak_rss`

*Type:* string or null. *Derived from the other fields.*

`peak_rss_bytes` in human-readable form, e.g. `1.2 GB`.

## TableDiffReport

A single table comparison, in the form saved in the PUDL Diff JSON report.

Built by `build_table_diff_report()` from a `TableDiffRun`, and one entry in `tables`. Fields that describe the whole comparison of the two datasets (when it was run, the datasets' provenance) live on the `PudlDiffReport` instead. Contains no row-level data itself - only counts and summaries; the actual differing rows are written separately as Parquet files (see `write_row_diff_parquet()`) and referenced from `row_diff`.

### `left_table_bytes`

*Type:* integer or null.

Size in bytes of the left table's Parquet file(s), or `null` if unknown.

### `right_table_bytes`

*Type:* integer or null.

Size in bytes of the right table's Parquet file(s), or `null` if unknown.

### `left_table_name`

*Type:* string.

The name of the table in the left dataset.

### `left_table_path`

*Type:* string.

The path or URL of the table's Parquet file in the left dataset. Worked out from the dataset's root and the table's name, so it is given even if the file doesn't exist, e.g. because the comparison failed. Absolute, for a dataset on the local filesystem.

### `right_table_name`

*Type:* string.

The name of the table in the right dataset. Differs from `left_table_name` only when two differently named tables were compared, e.g. a `core_` table against the `out_` table built from it.

### `right_table_path`

*Type:* string.

The path or URL of the table's Parquet file in the right dataset. Absolute, for a dataset on the local filesystem.

### `elapsed_seconds`

*Type:* number or null.

Wall-clock time the comparison of this table took, or `null` if it failed.

### `peak_rss_bytes`

*Type:* integer or null.

The most memory (resident set size) the process used during this comparison beyond what it was using when the comparison started, in bytes. Sampled, so a very short spike could be missed. `null` if the comparison failed.

### `peak_cpu_percent`

*Type:* number or null.

The highest CPU utilization sampled during this comparison, as a percentage of one core: `400.0` means four cores kept fully busy. A rough gauge of how parallel the work was. `null` if the comparison failed.

### `schema_diff`

*Type:* [SchemaDiffSummary](#schemadiffsummary) or null.

How the tables' columns and dtypes differ, or `null` if the comparison failed.

### `row_count_diff`

*Type:* [RowCountDiffSummary](#rowcountdiffsummary) or null.

How the tables' row counts differ, or `null` if the comparison failed.

### `row_diff`

*Type:* [RowDiffSummary](#rowdiffsummary) or null.

How the tables' rows differ, or `null` if the comparison failed. If the row-level comparison was skipped, its sections say why.

### `error`

*Type:* string or null.

Exception message plus traceback, if the comparison failed to complete. `null` if `success` is `true`.

### `left_table_size`

*Type:* string or null. *Derived from the other fields.*

`left_table_bytes` in human-readable form, e.g. `12.3 MB`.

### `right_table_size`

*Type:* string or null. *Derived from the other fields.*

`right_table_bytes` in human-readable form.

### `bytes_difference`

*Type:* integer or null. *Derived from the other fields.*

The change in size from the left to the right table.

`right_table_bytes - left_table_bytes`, so negative if the right side is smaller. Compression changes show up here even if the contents don't.

### `bytes_difference_size`

*Type:* string or null. *Derived from the other fields.*

`bytes_difference` in human-readable form, e.g. `-1.2 MB`.

### `bytes_difference_percent`

*Type:* number or null. *Derived from the other fields.*

`bytes_difference` as a percentage of `left_table_bytes`.

`null` if the left size is unknown or zero.

### `success`

*Type:* boolean. *Derived from the other fields.*

Whether the comparison completed at all, successfully or not.

See `TableDiffRun`. Distinct from `is_identical`: a comparison can succeed and still find the tables different.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether the table is functionally identical between the two datasets.

Conservatively `false` whenever `success` is `false`, since a failed comparison can't establish that the tables are identical, and whenever the row-level comparison didn't run (it was skipped), since then the rows are unverified even if the schema and row counts match.

### `peak_rss`

*Type:* string or null. *Derived from the other fields.*

`peak_rss_bytes` in human-readable form, e.g. `1.2 GB`.

## SchemaDiffSummary

The JSON-report form of `SchemaDiff`.

### `columns_only_in_left`

*Type:* array of string.

Names of the columns that are only in the left table: removed columns.

### `columns_only_in_right`

*Type:* array of string.

Names of the columns that are only in the right table: added columns.

### `dtype_changes`

*Type:* object mapping names to [string, string].

Maps column name to a `(left_dtype, right_dtype)` pair of dtype names, e.g. `("Int64", "Int32")`.

### `left_column_count`

*Type:* integer.

Number of columns in the left table.

### `right_column_count`

*Type:* integer.

Number of columns in the right table.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether the two schemas have the same columns and dtypes.

## RowCountDiffSummary

The JSON-report form of `RowCountDiff`.

### `left_row_count`

*Type:* integer.

Total number of rows in the left table.

### `right_row_count`

*Type:* integer.

Total number of rows in the right table.

### `row_count_difference`

*Type:* integer.

`right_row_count - left_row_count`: the change in row count from the reference (left) table to the right table.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether the row counts match.

## RowDiffSummary

The JSON-report form of `row_diff`.

Exactly one of `pk_diff` and `non_pk_diff` is a full summary (unless row-level comparison was skipped entirely); the other is a `RowDiffSectionSkipped` saying why it wasn't produced: `no_primary_key` or `primary_key_available` when the table's primary key determined which kind of comparison applies, or the reason the comparison was skipped altogether (see `RowComparisonSkipReason`), in which case the one that would have run carries that reason.

### `pk_diff`

*Type:* [PkRowDiffSummary](#pkrowdiffsummary) or [RowDiffSectionSkipped](#rowdiffsectionskipped), told apart by `status`.

The row-level comparison of a table with a primary key: a full summary (`status` is `"compared"`), or the reason there isn't one (`"skipped"`).

### `non_pk_diff`

*Type:* [NonPkRowDiffSummary](#nonpkrowdiffsummary) or [RowDiffSectionSkipped](#rowdiffsectionskipped), told apart by `status`.

The row-level comparison of a table without a primary key: a full summary (`status` is `"compared"`), or the reason there isn't one (`"skipped"`).

### `left_only_parquet`

*Type:* [ParquetOutputSummary](#parquetoutputsummary) or null.

The Parquet file of the rows found only in the left table (for a table with a primary key, this includes the left-hand values of rows that changed), or `null` if no file was written.

### `right_only_parquet`

*Type:* [ParquetOutputSummary](#parquetoutputsummary) or null.

The same, for the right table.

## PkRowDiffSummary

The JSON-report form of a `KeyedRowDiff`.

### `status`

*Type:* "compared".

Always `"compared"`: this is what tells this apart from a skipped section.

### `primary_key_columns`

*Type:* array of string.

The names of the table's primary key columns.

### `only_in_left_count`

*Type:* integer.

Number of rows whose primary key is only in the left table: removed rows.

### `only_in_right_count`

*Type:* integer.

Number of rows whose primary key is only in the right table: added rows.

### `changed_row_count`

*Type:* integer.

Number of shared-primary-key rows with at least one differing non-primary-key value.

### `column_changes`

*Type:* object mapping names to integer.

Maps each non-primary-key column to the number of shared-primary-key rows where its value differs between the tables. Columns with no changes are omitted.

### `primary_keys_identical`

*Type:* boolean. *Derived from the other fields.*

Whether both tables have the same set of primary keys.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether the primary keys match and no shared-key row has changed.

## RowDiffSectionSkipped

Stands in for a row diff summary that wasn't produced, and says why.

### `status`

*Type:* "skipped".

Always `"skipped"`: this is what tells this apart from a full summary.

### `skipped_reason`

*Type:* "too_many_rows" or "incompatible_dtypes" or "mismatched_columns" or "primary_key_available" or "no_primary_key".

Why there is no summary in this section:

* `too_many_rows`: either table has more rows than `DiffOptions.max_compare_rows`, so no row-level comparison was made.
* `incompatible_dtypes`: the row-level comparison failed, most likely because the tables' columns have incompatible dtypes.
* `mismatched_columns`: the tables have different columns and no primary key, so their rows can't be compared meaningfully.
* `primary_key_available`: not skipped for a problem. The table has a primary key, so its comparison is in `pk_diff`, not `non_pk_diff`.
* `no_primary_key`: likewise, the table has no primary key, so its comparison is in `non_pk_diff`, not `pk_diff`.

## NonPkRowDiffSummary

The JSON-report form of a `RowSetDiff` without a primary key.

### `status`

*Type:* "compared".

Always `"compared"`: this is what tells this apart from a skipped section.

### `only_in_left_count`

*Type:* integer.

Number of rows only in the left table: removed rows. Rows are counted as a multiset, so if a row appears more times on the left, the surplus copies count.

### `only_in_right_count`

*Type:* integer.

Number of rows only in the right table: added rows, counted the same way.

### `symmetric_difference_count`

*Type:* integer.

`only_in_left_count + only_in_right_count`. Counts rows as a multiset, so surplus copies of duplicated rows are included.

### `multiplicity_changed_row_count`

*Type:* integer.

Number of distinct rows present in both tables, but a different number of times.

### `is_identical`

*Type:* boolean. *Derived from the other fields.*

Whether every row in one table has a matching row in the other.

## ParquetOutputSummary

The JSON-report form of a single `ParquetOutput`.

### `path`

*Type:* string.

Where the file is, relative to the directory that contains the report (`pudl_diff_report.json`), so that the directory can be moved. Always written with `/` separators.

### `bytes`

*Type:* integer.

Size of the file in bytes.

### `hash`

*Type:* string.

`"sha256:<hexdigest>"` of the file's contents, the convention PUDL's `datapackage.json` uses for its own resource files.

### `size`

*Type:* string. *Derived from the other fields.*

`bytes` in human-readable form, e.g. `12.3 MB`.
