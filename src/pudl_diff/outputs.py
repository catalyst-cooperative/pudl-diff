"""Writing the rows that differ between two tables to Parquet files."""

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pudl.validate.diff.rows import (
    KeyedRowDiff,
    RowSetDiff,
)
from pudl.validate.diff.table import (
    row_diff_left_right_frames,
)


@dataclass(frozen=True)
class ParquetOutput:
    """Metadata about a single Parquet side-output file written to disk."""

    path: Path
    bytes: int
    hash: str
    """``"sha256:<hexdigest>"``, matching the convention PUDL's enriched
    ``datapackage.json`` uses for its own resource files."""
    total_row_count: int
    """Total number of differing rows on this side, before any capping by
    ``max_rows_per_output_parquet``."""
    rows_written: int
    """Number of rows actually written to :attr:`path`."""


@dataclass(frozen=True)
class RowDiffParquetOutputs:
    """The left-only and right-only Parquet files written for one table's row diff."""

    left: ParquetOutput
    right: ParquetOutput


def _sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet_output(
    lf: pl.LazyFrame,
    total_row_count: int,
    path: Path,
    max_rows_per_output_parquet: int | None,
) -> ParquetOutput:
    if max_rows_per_output_parquet is not None:
        lf = lf.head(max_rows_per_output_parquet)
    lf.sink_parquet(
        path,
        engine="streaming",
        # TODO: Update to pudl.PARQUET_COMPRESSION
        compression="zstd",
        # TODO: Update to pudl.PARQUET_COMPRESSION_LEVEL
        compression_level=3,
    )
    rows_written = (
        total_row_count
        if max_rows_per_output_parquet is None
        else min(total_row_count, max_rows_per_output_parquet)
    )
    return ParquetOutput(
        path=path,
        bytes=path.stat().st_size,
        hash=f"sha256:{_sha256_of_file(path)}",
        total_row_count=total_row_count,
        rows_written=rows_written,
    )


def write_row_diff_parquet(
    row_diff: RowSetDiff | KeyedRowDiff | None,
    output_path: str | os.PathLike[str],
    table_name: str,
    *,
    right_table_name: str | None = None,
    max_rows_per_output_parquet: int | None = None,
) -> RowDiffParquetOutputs | None:
    """Write a table's differing rows to left-only/right-only Parquet files.

    Writes ``<table_name>_left_only.parquet`` and
    ``<right_table_name>_right_only.parquet`` under ``output_path`` (created
    if it doesn't exist), each matching the schema of the corresponding
    source table. For a table with a primary key, each file holds that
    side's rows from the symmetric difference of primary keys, plus that
    side's values for rows whose non-PK data differs. For a table without
    one, each file holds that side's rows from the symmetric difference of
    whole rows.

    Args:
        row_diff: The row-level comparison result to write out, e.g. from
            :attr:`~.TableDiffResult.row_diff`. If ``None`` - row-level
            comparison was skipped for this table - nothing is written and
            this function returns ``None``.
        output_path: Directory to write the two Parquet files into.
        table_name: Used as the filename prefix for the left output file,
            and for the right one too unless ``right_table_name`` is given.
        right_table_name: Used as the filename prefix for the right output
            file, if it differs from ``table_name`` - e.g. writing out a
            diff between a `core_` table and the `out_` table built from it,
            as passed to :func:`~.compare_table`. Defaults to ``table_name``.
        max_rows_per_output_parquet: If given, caps the number of rows
            written to each file. :attr:`ParquetOutput.total_row_count`
            still reflects the true (uncapped) row count.

    Returns:
        The two files' paths and metadata, or ``None`` if ``row_diff`` is
        ``None``.
    """
    if row_diff is None:
        return None
    right_table_name = right_table_name or table_name
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    (left_lf, left_count), (right_lf, right_count) = row_diff_left_right_frames(
        row_diff
    )
    return RowDiffParquetOutputs(
        left=_write_parquet_output(
            left_lf,
            left_count,
            output_path / f"{table_name}_left_only.parquet",
            max_rows_per_output_parquet,
        ),
        right=_write_parquet_output(
            right_lf,
            right_count,
            output_path / f"{right_table_name}_right_only.parquet",
            max_rows_per_output_parquet,
        ),
    )
