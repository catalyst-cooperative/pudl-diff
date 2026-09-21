"""Comparing the rows of two tables, with or without a primary key."""

import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from pudl_diff.row_counts import (
    count_rows,
)

_ROW_KEY_COL = "_pudl_diff_row_key"
_LEFT_COUNT_COL = "_pudl_diff_left_count"
_RIGHT_COUNT_COL = "_pudl_diff_right_count"
_LEFT_SURPLUS_COL = "_pudl_diff_left_surplus"
_RIGHT_SURPLUS_COL = "_pudl_diff_right_surplus"


def _floats_close_enough(rtol: float, atol: float) -> bool:
    return rtol > 0 or atol > 0


def _row_key_field(name: str, dtype: pl.DataType, rtol: float, atol: float) -> pl.Expr:
    """A struct field expression used to build a row's key hash.

    Float columns are quantized into buckets sized by ``rtol``/``atol`` so that
    two "close enough" values (per :func:`numpy.isclose`'s definition) fall into
    the same bucket and are therefore treated as matching for the purposes of
    finding rows that exist in both tables. This is a best-effort approximation,
    not an exact reproduction of pairwise ``isclose`` comparisons, since it hard
    -partitions the number line instead of comparing every pair of values.
    """
    if not dtype.is_float() or not _floats_close_enough(rtol, atol):
        return pl.col(name)
    scale = atol + rtol * pl.col(name).abs()
    return (
        pl.when(pl.col(name).is_null())
        .then(None)
        # `scale` overflows to inf for an infinite input, so the bucket below
        # would divide inf by inf (NaN) - key infinities on their own exact
        # value instead, which reproduces numpy.isclose's treatment of them
        # (only equal to another infinity of the same sign).
        .when(pl.col(name).is_infinite())
        .then(pl.col(name))
        .otherwise((pl.col(name) / scale).round(0))
        .alias(name)
    )


def _with_row_key(
    lf: pl.LazyFrame,
    schema: pl.Schema,
    key_columns: Iterable[str],
    rtol: float,
    atol: float,
) -> pl.LazyFrame:
    """Add a 64-bit hash of each row's (float-quantized) key columns.

    Joining on this single ``UInt64`` column instead of a struct of the key
    columns keeps the join's in-memory side to 8 bytes per row no matter how many
    or how wide the key columns are. Polars' hash treats nulls, ``0.0``/``-0.0``
    and differing NaN payloads as equal, matching how a struct join key would.
    Hashes of different dtypes differ though, so callers must first check that
    the key columns' dtypes match (see :func:`_assert_key_dtypes_match`).

    A 64-bit hash can collide: for two different keys the chance is about
    2**-64, so at hundreds of millions of rows the chance of a real change being
    masked by a collision is negligible (around 1e-11 per changed row).
    """
    fields = [_row_key_field(name, schema[name], rtol, atol) for name in key_columns]
    return lf.with_columns(pl.struct(fields).hash().alias(_ROW_KEY_COL))


def _assert_key_dtypes_match(
    left_schema: pl.Schema, right_schema: pl.Schema, key_columns: Iterable[str]
) -> None:
    """Raise :class:`polars.exceptions.SchemaError` if key column dtypes differ.

    A struct join key raises on mismatched dtypes, but hashes of e.g. ``Int32`` and
    ``Int64`` values just silently differ, which would report every row as changed.
    """
    for name in key_columns:
        left_dtype = left_schema[name]
        right_dtype = right_schema.get(name)
        if left_dtype != right_dtype:
            raise pl.exceptions.SchemaError(
                f"Key column {name!r} has dtype {left_dtype} on the left but "
                f"{right_dtype} on the right."
            )


def _spill(
    lf: pl.LazyFrame, spill_dir: tempfile.TemporaryDirectory[str], name: str
) -> pl.LazyFrame:
    """Stream ``lf``'s result to Parquet in ``spill_dir``, and scan it back lazily.

    Keeps potentially huge diff results on disk rather than in memory, and lets
    them be read back (or counted, from Parquet metadata) as many times as needed
    without re-running the comparison that produced them.
    """
    path = Path(spill_dir.name) / f"{name}.parquet"
    lf.sink_parquet(path, engine="streaming")
    return pl.scan_parquet(path)


@dataclass(frozen=True)
class RowSetDiff:
    """The multiset symmetric difference of rows between two tables.

    A row only counts as "the same" if the key columns match on both sides -
    every column, for tables with no primary key, or just the primary key columns,
    for tables that have one. For tables with no primary key, the number of
    copies of each row matters too: a row appearing 3 times on the left and once
    on the right contributes 2 rows to :attr:`only_in_left`.

    The differing rows themselves are backed by temporary Parquet files, so they
    can be far larger than memory. The files are deleted when this object (and any
    other object sharing :attr:`spill_dir`) is garbage collected; call
    ``.collect()`` on a frame to load it.
    """

    only_in_left: pl.LazyFrame
    only_in_right: pl.LazyFrame
    only_in_left_count: int
    only_in_right_count: int
    multiplicity_changed_row_count: int
    """Number of distinct rows present on both sides but a different number of
    times (only ever non-zero for tables with no primary key)."""
    spill_dir: tempfile.TemporaryDirectory[str] = field(repr=False, compare=False)

    @property
    def is_identical(self) -> bool:
        """Whether every row in one table has a matching row in the other."""
        return self.only_in_left_count == 0 and self.only_in_right_count == 0


@dataclass(frozen=True)
class _KeyDelta:
    """Intermediate result of comparing two tables by key hash."""

    row_set_diff: RowSetDiff
    affected: pl.LazyFrame
    """One row per key hash whose row count or value hash differs between the
    two tables, with per-side row counts and surplus row counts."""
    left_keyed: pl.LazyFrame
    right_keyed: pl.LazyFrame


def _reduce_rows(
    lf: pl.LazyFrame,
    schema: pl.Schema,
    key_columns: list[str],
    value_columns: list[str],
    rtol: float,
    atol: float,
    side: str,
) -> pl.LazyFrame:
    """Reduce a table to one narrow row per key hash.

    Each row holds the number of rows sharing that key hash and, if there are
    value columns, an order-independent hash of their values.
    """
    keyed = _with_row_key(lf, schema, key_columns, rtol, atol)
    aggs = [pl.len().alias(f"_pudl_diff_{side}_count")]
    if value_columns:
        # Hash the exact values, not float-quantized ones: equal hashes then
        # guarantee equal values, so no real change can hide behind a bucket
        # boundary. Hashes that differ only flag a row for exact comparison.
        # Summing (wrapping on overflow) makes the per-key hash independent of
        # row order, for the (invalid, but possible) case of duplicate keys.
        keyed = keyed.with_columns(pl.struct(value_columns).hash().alias("_vh"))
        aggs.append(pl.col("_vh").sum().alias(f"_pudl_diff_{side}_value_hash"))
    return keyed.group_by(_ROW_KEY_COL).agg(aggs)


def _semi_rows(
    keyed: pl.LazyFrame, affected: pl.LazyFrame, predicate: pl.Expr
) -> pl.LazyFrame:
    """Rows of ``keyed`` whose key hash is in ``affected`` where ``predicate``."""
    keys = affected.filter(predicate).select(_ROW_KEY_COL)
    return keyed.join(keys, on=_ROW_KEY_COL, how="semi")


def _surplus_rows(
    keyed: pl.LazyFrame,
    affected: pl.LazyFrame,
    columns: list[str],
    spill_dir: tempfile.TemporaryDirectory[str],
    name: str,
    other_count_col: str,
    surplus_col: str,
    multiset: bool,
) -> pl.LazyFrame:
    """The rows on one side that have no counterpart on the other side.

    That's every row whose key is absent from the other side and, if ``multiset``,
    the surplus copies of rows present on both sides more times on this one.
    """
    one_sided = _spill(
        _semi_rows(keyed, affected, pl.col(other_count_col) == 0).select(columns),
        spill_dir,
        f"{name}_one_sided",
    )
    if not multiset:
        return one_sided
    surplus_keys = affected.filter(
        (pl.col(other_count_col) > 0) & (pl.col(surplus_col) > 0)
    ).select(_ROW_KEY_COL, surplus_col)
    extra_copies = _spill(
        keyed.join(surplus_keys, on=_ROW_KEY_COL, how="inner")
        .filter(pl.int_range(pl.len()).over(_ROW_KEY_COL) < pl.col(surplus_col))
        .select(columns),
        spill_dir,
        f"{name}_extra_copies",
    )
    return pl.concat([one_sided, extra_copies])


def _diff_by_key_hash(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    schema: pl.Schema,
    key_columns: Iterable[str],
    rtol: float,
    atol: float,
    spill_dir: tempfile.TemporaryDirectory[str],
    *,
    value_columns: Sequence[str] = (),
    multiset: bool,
) -> _KeyDelta:
    """Find the rows that differ between two tables by joining narrow key hashes.

    Each table is first reduced to one small row per key hash (see
    :func:`_reduce_rows`) and written to disk, so that only those narrow frames,
    rather than the tables' full rows, are held in memory by the join that finds
    the differing keys. Full rows are then read back only for those keys.

    Args:
        key_columns: The columns identifying a row: the primary key, or every
            column for a table with none.
        value_columns: The non-key columns to also compare between rows sharing a
            key. Rows whose exact-value hashes differ are only *candidates* for a
            change; callers must compare them properly (e.g. with a tolerance).
        multiset: Whether the number of copies of a key matters. If so, surplus
            copies on either side are reported as one-sided rows; if not, only
            keys entirely missing from a side are.
    """
    columns = list(schema.keys())
    key_columns = list(key_columns)
    value_columns = list(value_columns)
    _assert_key_dtypes_match(schema, right.collect_schema(), key_columns)

    left_reduced = _spill(
        _reduce_rows(left, schema, key_columns, value_columns, rtol, atol, "left"),
        spill_dir,
        "left_reduced",
    )
    right_reduced = _spill(
        _reduce_rows(right, schema, key_columns, value_columns, rtol, atol, "right"),
        spill_dir,
        "right_reduced",
    )

    left_count = pl.col(_LEFT_COUNT_COL).fill_null(0).cast(pl.Int64)
    right_count = pl.col(_RIGHT_COUNT_COL).fill_null(0).cast(pl.Int64)
    differs = left_count != right_count
    if value_columns:
        differs = differs | (
            pl.col("_pudl_diff_left_value_hash")
            != pl.col("_pudl_diff_right_value_hash")
        ).fill_null(False)
    if multiset:
        left_surplus = (left_count - right_count).clip(lower_bound=0)
        right_surplus = (right_count - left_count).clip(lower_bound=0)
    else:
        left_surplus = pl.when(right_count == 0).then(left_count).otherwise(0)
        right_surplus = pl.when(left_count == 0).then(right_count).otherwise(0)
    affected = _spill(
        left_reduced.join(right_reduced, on=_ROW_KEY_COL, how="full", coalesce=True)
        .filter(differs)
        .select(
            _ROW_KEY_COL,
            left_count.alias(_LEFT_COUNT_COL),
            right_count.alias(_RIGHT_COUNT_COL),
            left_surplus.alias(_LEFT_SURPLUS_COL),
            right_surplus.alias(_RIGHT_SURPLUS_COL),
        ),
        spill_dir,
        "affected",
    )
    left_total, right_total, multiplicity_changed = (
        affected.select(
            pl.col(_LEFT_SURPLUS_COL).sum(),
            pl.col(_RIGHT_SURPLUS_COL).sum(),
            ((pl.col(_LEFT_COUNT_COL) > 0) & (pl.col(_RIGHT_COUNT_COL) > 0)).sum(),
        )
        .collect(engine="streaming")
        .row(0)
    )

    left_keyed = _with_row_key(left, schema, key_columns, rtol, atol)
    right_keyed = _with_row_key(right, schema, key_columns, rtol, atol)
    row_set_diff = RowSetDiff(
        only_in_left=_surplus_rows(
            left_keyed,
            affected,
            columns,
            spill_dir,
            "only_in_left",
            _RIGHT_COUNT_COL,
            _LEFT_SURPLUS_COL,
            multiset,
        ),
        only_in_right=_surplus_rows(
            right_keyed,
            affected,
            columns,
            spill_dir,
            "only_in_right",
            _LEFT_COUNT_COL,
            _RIGHT_SURPLUS_COL,
            multiset,
        ),
        only_in_left_count=left_total,
        only_in_right_count=right_total,
        multiplicity_changed_row_count=multiplicity_changed if multiset else 0,
        spill_dir=spill_dir,
    )
    return _KeyDelta(row_set_diff, affected, left_keyed, right_keyed)


def compare_rows_without_pk(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    *,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> RowSetDiff:
    """Compare two tables with no primary key by taking the symmetric difference of rows.

    The comparison is of multisets: a row that appears a different number of times
    in the two tables counts as a difference, and the surplus copies are reported
    as only in the table that has more of them.

    Args:
        left: The "left" table to compare. Must have the same columns as
            ``right``, with the same dtypes.
        right: The "right" table to compare against ``left``.
        rtol: Relative tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default. Set to ``0`` along
            with ``atol`` to require exact float equality.
        atol: Absolute tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default.

    Raises:
        polars.exceptions.SchemaError: if a column's dtype differs between
            ``left`` and ``right``.
    """
    schema = left.collect_schema()
    spill_dir = tempfile.TemporaryDirectory(prefix="pudl_diff_")
    return _diff_by_key_hash(
        left, right, schema, schema.keys(), rtol, atol, spill_dir, multiset=True
    ).row_set_diff


def _values_differ(
    left_name: str, right_name: str, dtype: pl.DataType, rtol: float, atol: float
) -> pl.Expr:
    """A boolean expression that's ``True`` where two columns' values differ.

    Two nulls are considered equal to each other. Float columns are compared
    with :func:`numpy.isclose`-style tolerance; everything else, exactly.
    """
    left_e = pl.col(left_name)
    right_e = pl.col(right_name)
    left_null = left_e.is_null()
    right_null = right_e.is_null()
    both_null = left_null & right_null
    # Exactly one side is null. Compared with `!=` rather than `^` since both
    # operands are themselves guaranteed non-null booleans, avoiding the null
    # propagation that would otherwise poison `close` below when only one of
    # `left_e`/`right_e` is null.
    one_null = left_null != right_null
    if dtype.is_float():
        # Handle infinities via exact equality rather than the tolerance check
        # below, matching numpy.isclose's own treatment of them: two
        # infinities of the same sign are equal even though their difference
        # is NaN (inf - inf), while an infinity and a finite value (or two
        # opposite-signed infinities) are never close, even though the scale
        # below also overflows to inf, which would otherwise wrongly satisfy
        # `inf <= inf`.
        both_finite = left_e.is_finite() & right_e.is_finite()
        close = (left_e == right_e) | (
            both_finite & ((left_e - right_e).abs() <= (atol + rtol * right_e.abs()))
        )
    else:
        close = left_e == right_e
    equal = both_null | (close & ~one_null)
    return ~equal


@dataclass(frozen=True)
class KeyedRowDiff:
    """Row-level differences between two tables that share a primary key.

    Like :class:`RowSetDiff`, the differing rows are backed by temporary Parquet
    files (owned by :attr:`pk_diff`) rather than held in memory.
    """

    pk_diff: RowSetDiff
    """Symmetric difference of primary key values: full rows present in only
    one of the two tables."""

    column_changes: dict[str, int]
    """Maps each non-primary-key column to the number of shared-primary-key
    rows where its value differs between the two tables. Columns with no
    changes are omitted."""

    changed_row_count: int
    """Number of shared-primary-key rows with at least one differing
    non-primary-key value."""

    changed_left: pl.LazyFrame
    """Rows sharing a primary key whose non-primary-key values differ,
    holding the left table's values, with the same schema as the left
    table."""

    changed_right: pl.LazyFrame
    """Same as :attr:`changed_left`, but holding the right table's values
    for those same primary keys, with the same schema as the right table."""

    @property
    def is_identical(self) -> bool:
        """Whether every shared-key row matches and no keys are one-sided."""
        return self.pk_diff.is_identical and not self.column_changes


def compare_rows_with_pk(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    pk_cols: Sequence[str],
    *,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> KeyedRowDiff:
    """Compare two tables sharing a primary key.

    Args:
        left: The "left" table to compare. Must have the same columns as
            ``right``, with matching dtypes for the primary key columns.
        right: The "right" table to compare against ``left``.
        pk_cols: Names of the primary key columns shared by both tables, e.g.
            from ``PudlDiffDataset.primary_key(table_name)``.
        rtol: Relative tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default.
        atol: Absolute tolerance used to treat two floating point values as
            equal, matching :func:`numpy.isclose`'s default.
    """
    schema = left.collect_schema()
    columns = list(schema.keys())
    non_pk_cols = [name for name in schema if name not in pk_cols]
    left_cols = [f"{c}_left" for c in non_pk_cols]
    right_cols = [f"{c}_right" for c in non_pk_cols]

    spill_dir = tempfile.TemporaryDirectory(prefix="pudl_diff_")
    delta = _diff_by_key_hash(
        left,
        right,
        schema,
        pk_cols,
        rtol,
        atol,
        spill_dir,
        value_columns=non_pk_cols,
        multiset=False,
    )

    # Only keys present on both sides whose non-key values hash differently can
    # have changed, so compare just those rows, exactly and with tolerance.
    on_both_sides = (pl.col(_LEFT_COUNT_COL) > 0) & (pl.col(_RIGHT_COUNT_COL) > 0)
    left_candidates = _spill(
        _semi_rows(delta.left_keyed, delta.affected, on_both_sides).select(columns),
        spill_dir,
        "left_candidates",
    )
    right_candidates = _spill(
        _semi_rows(delta.right_keyed, delta.affected, on_both_sides).select(columns),
        spill_dir,
        "right_candidates",
    )
    left_renamed = left_candidates.rename(
        dict(zip(non_pk_cols, left_cols, strict=True))
    )
    right_renamed = right_candidates.rename(
        dict(zip(non_pk_cols, right_cols, strict=True))
    )
    joined = left_renamed.join(right_renamed, on=list(pk_cols), how="inner")
    diff_flag_cols = [f"_pudl_diff_flag_{c}" for c in non_pk_cols]
    joined = joined.with_columns(
        [
            _values_differ(lc, rc, schema[c], rtol, atol).alias(flag_col)
            for c, lc, rc, flag_col in zip(
                non_pk_cols, left_cols, right_cols, diff_flag_cols, strict=True
            )
        ]
    )

    # Keep only the rows that really changed (both sides' values and the
    # per-column flags). Everything below - the counts and both output frames -
    # is derived from this small spilled result.
    any_diff = pl.any_horizontal(diff_flag_cols) if diff_flag_cols else pl.lit(False)
    changed = _spill(joined.filter(any_diff), spill_dir, "changed")

    counts = (
        changed.select([pl.col(fc).sum() for fc in diff_flag_cols]).collect(
            engine="streaming"
        )
        if non_pk_cols
        else pl.DataFrame()
    )
    column_changes = {
        c: count
        for c, fc in zip(non_pk_cols, diff_flag_cols, strict=True)
        if (count := counts[fc][0]) > 0
    }

    changed_left = (
        changed.select([*pk_cols, *left_cols])
        .rename(dict(zip(left_cols, non_pk_cols, strict=True)))
        .select(columns)
    )
    changed_right = (
        changed.select([*pk_cols, *right_cols])
        .rename(dict(zip(right_cols, non_pk_cols, strict=True)))
        .select(columns)
    )

    return KeyedRowDiff(
        pk_diff=delta.row_set_diff,
        column_changes=column_changes,
        changed_row_count=count_rows(changed),
        changed_left=changed_left,
        changed_right=changed_right,
    )
