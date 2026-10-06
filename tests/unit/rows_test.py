"""Unit tests for pudl_diff.rows."""

import gc
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pudl_diff.rows import (
    SpillDir,
    compare_rows_with_pk,
    compare_rows_without_pk,
)


def test_compare_rows_without_pk_identical():
    """Compare rows without a primary key identical."""
    left = pl.LazyFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    right = pl.LazyFrame({"x": [3, 1, 2], "y": ["c", "a", "b"]})
    result = compare_rows_without_pk(left, right)
    assert result.is_identical
    assert result.only_in_left.collect().is_empty()
    assert result.only_in_right.collect().is_empty()


def test_compare_rows_without_pk_added_and_removed_rows():
    """Compare rows without a primary key added and removed rows."""
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [1, 2, 4]})
    result = compare_rows_without_pk(left, right)
    assert not result.is_identical
    assert result.only_in_left.collect()["x"].to_list() == [3]
    assert result.only_in_right.collect()["x"].to_list() == [4]


def test_compare_rows_without_pk_changed_row():
    """Compare rows without a primary key changed row."""
    left = pl.LazyFrame({"x": [1, 2], "y": ["a", "b"]})
    right = pl.LazyFrame({"x": [1, 2], "y": ["a", "changed"]})
    result = compare_rows_without_pk(left, right)
    assert result.only_in_left.collect().to_dicts() == [{"x": 2, "y": "b"}]
    assert result.only_in_right.collect().to_dicts() == [{"x": 2, "y": "changed"}]


def test_compare_rows_without_pk_float_within_tolerance():
    """Floats that differ by less than the default tolerance are equal.

    The two values differ by 1e-8, well inside `atol + rtol * abs(y)` for the default
    `rtol=1e-5` and `atol=1e-8`, so the rows match, as `numpy.isclose()` says they do.
    """
    left = pl.LazyFrame({"x": [1], "y": [1.00000001]})
    right = pl.LazyFrame({"x": [1], "y": [1.00000002]})
    assert compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_float_outside_tolerance():
    """Compare rows without a primary key float outside tolerance."""
    left = pl.LazyFrame({"x": [1], "y": [1.0]})
    right = pl.LazyFrame({"x": [1], "y": [2.0]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_exact_float_equality():
    """With both tolerances at zero, any difference in a float makes rows differ.

    Zero tolerances turn off the bucketing of floats, so the same values that are equal
    under the default tolerance are now a removed row and an added one.
    """
    left = pl.LazyFrame({"y": [1.00000001]})
    right = pl.LazyFrame({"y": [1.00000002]})
    result = compare_rows_without_pk(left, right, rtol=0, atol=0)
    assert not result.is_identical


def test_compare_rows_without_pk_same_sign_infinity():
    """An infinity equals an infinity of the same sign.

    Rows are matched by hashing floats in buckets sized by the tolerance, which can't be
    done for an infinity (its bucket would be inf / inf). Infinities are hashed as
    themselves instead, to match `numpy.isclose()`.
    """
    left = pl.LazyFrame({"y": [float("inf")]})
    right = pl.LazyFrame({"y": [float("inf")]})
    assert compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_opposite_sign_infinity():
    """Infinities of opposite signs are different."""
    left = pl.LazyFrame({"y": [float("-inf")]})
    right = pl.LazyFrame({"y": [float("inf")]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_infinity_vs_finite():
    """An infinity is different from any finite number, however large.

    The tolerance scales with the size of the value, so it would be infinite for an
    infinity, and would wrongly make every finite value "close" to it if the comparison
    didn't treat infinities as a special case.
    """
    left = pl.LazyFrame({"y": [float("inf")]})
    right = pl.LazyFrame({"y": [1e10]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_counts_and_spill_cleanup():
    """The counts of one-sided rows are right, and the rows' files go when the diff does.

    The differing rows are written to Parquet files in a temporary directory, so that
    tables too big for memory can be compared. The directory should be deleted once the
    result that owns it is garbage collected, and not left behind.
    """
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [3, 4]})
    result = compare_rows_without_pk(left, right)
    assert result.only_in_left_count == 2
    assert result.only_in_right_count == 1
    spill_path = Path(result.spill_dir.name)
    assert spill_path.exists()
    del result
    gc.collect()
    assert not spill_path.exists()


def test_compare_rows_with_pk_counts():
    """A table with a primary key reports its added, removed and changed rows separately.

    Key 1 is only on the left, key 4 only on the right, and key 3 is in both with a
    different `val`, which is a change to a row rather than a removed row and an added
    one, and is counted per column.
    """
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1, 2, 3]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2, 30, 4]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.pk_diff.only_in_left_count == 1
    assert result.pk_diff.only_in_right_count == 1
    assert result.changed_row_count == 1
    assert result.column_changes == {"val": 1}


def test_compare_rows_without_pk_multiplicity_change():
    """Without a primary key, rows are compared as a multiset.

    `x=1` appears three times on the left and once on the right, and `x=2` once and twice.
    The surplus copies are the differences: two `1`s only on the left, and one `2` only on
    the right. `multiplicity_changed_row_count` counts the distinct rows whose number of
    copies changed, which is two.
    """
    left = pl.LazyFrame({"x": [1, 1, 1, 2]})
    right = pl.LazyFrame({"x": [1, 2, 2]})
    result = compare_rows_without_pk(left, right)
    assert not result.is_identical
    assert result.only_in_left.collect()["x"].to_list() == [1, 1]
    assert result.only_in_right.collect()["x"].to_list() == [2]
    assert result.only_in_left_count == 2
    assert result.only_in_right_count == 1
    assert result.multiplicity_changed_row_count == 2


def test_compare_rows_without_pk_duplicates_only_on_one_side():
    """A row with no copy at all on one side is not a change of multiplicity.

    Both surplus copies of `x=5` are only on the left, but as that row is missing from the
    right, and not present a different number of times, it doesn't count towards
    `multiplicity_changed_row_count`.
    """
    left = pl.LazyFrame({"x": [5, 5, 1]})
    right = pl.LazyFrame({"x": [1]})
    result = compare_rows_without_pk(left, right)
    assert result.only_in_left.collect()["x"].to_list() == [5, 5]
    assert result.only_in_right_count == 0
    assert result.multiplicity_changed_row_count == 0


def test_compare_rows_without_pk_same_duplicates_are_identical():
    """Compare rows without a primary key same duplicates are identical."""
    left = pl.LazyFrame({"x": [1, 1, 2], "y": ["a", "a", "b"]})
    right = pl.LazyFrame({"x": [2, 1, 1], "y": ["b", "a", "a"]})
    assert compare_rows_without_pk(left, right).is_identical


@pytest.mark.parametrize("seed", range(20))
def test_compare_rows_without_pk_matches_multiset_oracle(seed: int):
    """On random tables, the rows only on each side are the multiset differences.

    A brute-force oracle: `collections.Counter`'s subtraction of the rows of one table from
    the other's gives the rows only in each, with surplus copies, and the number of rows in
    both whose number of copies differs. The tables are small, with a few distinct values,
    so that duplicates are common.
    """
    rng = np.random.default_rng(seed)

    def random_rows() -> list[tuple[int, str]]:
        n = rng.integers(0, 30)
        return [
            (int(rng.integers(0, 5)), str(rng.choice(["a", "b"]))) for _ in range(n)
        ]

    left_rows, right_rows = random_rows(), random_rows()
    schema = {"x": pl.Int64, "y": pl.String}
    left = pl.LazyFrame(left_rows, schema=schema, orient="row")
    right = pl.LazyFrame(right_rows, schema=schema, orient="row")

    result = compare_rows_without_pk(left, right)

    left_counts, right_counts = Counter(left_rows), Counter(right_rows)
    assert Counter(result.only_in_left.collect().rows()) == left_counts - right_counts
    assert Counter(result.only_in_right.collect().rows()) == right_counts - left_counts
    assert result.multiplicity_changed_row_count == sum(
        1
        for row in left_counts.keys() & right_counts.keys()
        if left_counts[row] != right_counts[row]
    )


def test_compare_rows_without_pk_hash_semantics_match_equal_values():
    """Values a struct join treats as equal must hash as equal too."""
    left = pl.LazyFrame(
        {"x": [0.0, float("nan"), None], "y": ["a", "b", None]},
    )
    right = pl.LazyFrame(
        {"x": [-0.0, -float("nan"), None], "y": ["a", "b", None]},
    )
    assert compare_rows_without_pk(left, right, rtol=0, atol=0).is_identical
    assert compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_mismatched_dtypes_raise():
    """Tables whose columns have different dtypes can't be compared.

    The rows are matched by hashing them, and hashes of `Int32` and `Int64` values differ,
    so this would report every row as changed. It raises instead.
    """
    left = pl.LazyFrame({"x": pl.Series([1, 2], dtype=pl.Int32)})
    right = pl.LazyFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
    with pytest.raises(pl.exceptions.SchemaError):
        compare_rows_without_pk(left, right)


def test_compare_rows_with_pk_mismatched_key_dtypes_raise():
    """A primary key with a different dtype on each side can't be compared.

    As with a table with no primary key, hashes of the keys would silently differ, and
    every row would look removed and added.
    """
    left = pl.LazyFrame(
        {"id": pl.Series([1, 2], dtype=pl.Int32), "val": [1, 2]},
    )
    right = pl.LazyFrame(
        {"id": pl.Series([1, 2], dtype=pl.Int64), "val": [1, 2]},
    )
    with pytest.raises(pl.exceptions.SchemaError):
        compare_rows_with_pk(left, right, ["id"])


def test_compare_rows_with_pk_tolerates_differing_non_key_dtypes():
    """Only the primary key's dtypes have to match.

    The other columns are compared value by value, so `Int32` and `Int64` values that are
    equal are equal.
    """
    left = pl.LazyFrame({"id": [1, 2], "val": pl.Series([1, 2], dtype=pl.Int32)})
    right = pl.LazyFrame({"id": [1, 2], "val": pl.Series([1, 3], dtype=pl.Int64)})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}
    assert result.changed_row_count == 1


def test_compare_rows_with_pk_hash_semantics_match_equal_values():
    """Compare rows with a primary key hash semantics match equal values."""
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [0.0, float("nan"), None]})
    right = pl.LazyFrame({"id": [1, 2, 3], "val": [-0.0, -float("nan"), None]})
    assert compare_rows_with_pk(left, right, ["id"], rtol=0, atol=0).is_identical


def test_compare_rows_with_pk_duplicate_keys_are_counted():
    """Both copies of a key that's duplicated on one side are counted as only there."""
    left = pl.LazyFrame({"id": [1, 1, 2], "val": [1, 1, 2]})
    right = pl.LazyFrame({"id": [2], "val": [2]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.pk_diff.only_in_left_count == 2
    assert result.pk_diff.only_in_right_count == 0


def test_compare_rows_with_pk_float_within_tolerance_is_not_a_change():
    """Exact value hashes differ, but the tolerance-aware comparison must clear it."""
    left = pl.LazyFrame({"id": [1], "val": [1.00000001]})
    right = pl.LazyFrame({"id": [1], "val": [1.00000002]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.changed_row_count == 0


def test_compare_rows_with_pk_identical():
    """Compare rows with a primary key identical."""
    left = pl.LazyFrame({"id": [1, 2], "val": [1.0, 2.0], "name": ["a", "b"]})
    right = pl.LazyFrame({"id": [2, 1], "val": [2.0, 1.0], "name": ["b", "a"]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.pk_diff.is_identical
    assert result.column_changes == {}
    assert result.changed_row_count == 0


def test_compare_rows_with_pk_differing_key_sets():
    """Compare rows with a primary key differing key sets."""
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1.0, 2.0, 3.0]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2.0, 3.0, 4.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.pk_diff.only_in_left.collect()["id"].to_list() == [1]
    assert result.pk_diff.only_in_right.collect()["id"].to_list() == [4]
    assert result.column_changes == {}


def test_compare_rows_with_pk_left_only_and_right_only():
    """Compare rows with a primary key left only and right only."""
    left = pl.LazyFrame({"id": [1, 2]})
    right = pl.LazyFrame({"id": [1]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.pk_diff.only_in_left.collect()["id"].to_list() == [2]
    assert result.pk_diff.only_in_right.collect().is_empty()


def test_compare_rows_with_pk_numeric_column_mismatch():
    """Compare rows with a primary key numeric column mismatch."""
    left = pl.LazyFrame({"id": [1, 2], "val": [1, 2]})
    right = pl.LazyFrame({"id": [1, 2], "val": [1, 99]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.pk_diff.is_identical
    assert result.column_changes == {"val": 1}
    assert result.changed_left.collect().to_dicts() == [{"id": 2, "val": 2}]
    assert result.changed_right.collect().to_dicts() == [{"id": 2, "val": 99}]


def test_compare_rows_with_pk_string_column_mismatch():
    """Compare rows with a primary key string column mismatch."""
    left = pl.LazyFrame({"id": [1, 2], "name": ["a", "b"]})
    right = pl.LazyFrame({"id": [1, 2], "name": ["a", "changed"]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"name": 1}
    assert result.changed_left.collect().to_dicts() == [{"id": 2, "name": "b"}]
    assert result.changed_right.collect().to_dicts() == [{"id": 2, "name": "changed"}]


def test_compare_rows_with_pk_float_within_tolerance():
    """Floats within the tolerance are not a change, and aren't counted as one."""
    left = pl.LazyFrame({"id": [1], "val": [1.00000001]})
    right = pl.LazyFrame({"id": [1], "val": [1.00000002]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.column_changes == {}


def test_compare_rows_with_pk_float_outside_tolerance():
    """Compare rows with a primary key float outside tolerance."""
    left = pl.LazyFrame({"id": [1], "val": [1.0]})
    right = pl.LazyFrame({"id": [1], "val": [2.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.column_changes == {"val": 1}


def test_compare_rows_with_pk_same_sign_infinity():
    """Compare rows with a primary key same sign infinity."""
    left = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    right = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.column_changes == {}


def test_compare_rows_with_pk_opposite_sign_infinity():
    """Compare rows with a primary key opposite sign infinity."""
    left = pl.LazyFrame({"id": [1], "val": [float("-inf")]})
    right = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}


def test_compare_rows_with_pk_null_equality():
    """A null equals a null."""
    left = pl.LazyFrame({"id": [1, 2], "val": [None, 1.0]})
    right = pl.LazyFrame({"id": [1, 2], "val": [None, 1.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical


def test_compare_rows_with_pk_null_vs_value_mismatch():
    """A null is different from a value.

    The left column's dtype is given explicitly, since a column of only nulls has none.
    """
    left = pl.LazyFrame(
        {"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Float64}
    )
    right = pl.LazyFrame({"id": [1], "val": [1.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}


def _shuffled(df: pl.DataFrame, seed: int) -> pl.DataFrame:
    """`df` with its rows and its columns in a different, random order."""
    rng = np.random.default_rng(seed)
    columns = [df.columns[i] for i in rng.permutation(len(df.columns))]
    return df[rng.permutation(df.height)].select(columns)


@pytest.mark.parametrize("seed", range(10))
def test_compare_rows_without_pk_ignores_row_and_column_order(seed: int):
    """Tables with the same rows are identical, however their rows and columns are ordered.

    Repeated on random tables, with many duplicate rows, and with the right table's rows
    and columns shuffled.
    """
    rng = np.random.default_rng(seed)
    n = int(rng.integers(5, 40))
    # Small value ranges, so there are plenty of duplicate rows.
    df = pl.DataFrame(
        {
            "x": rng.integers(0, 4, n),
            "y": rng.choice(["a", "b", "c"], n),
            "z": rng.integers(0, 3, n) / 2,
        }
    )
    result = compare_rows_without_pk(df.lazy(), _shuffled(df, seed + 1).lazy())
    assert result.is_identical
    assert result.only_in_left.collect().height == 0
    assert result.only_in_right.collect().height == 0


@pytest.mark.parametrize("seed", range(10))
def test_compare_rows_with_pk_ignores_row_and_column_order(seed: int):
    """Tables with the same rows are identical, however their rows and columns are ordered.

    Repeated on random tables, with a two-column key given in either order, and compared
    both ways round.
    """
    rng = np.random.default_rng(seed)
    n = int(rng.integers(5, 40))
    df = pl.DataFrame(
        {
            "k1": np.repeat(np.arange(n // 2 + 1), 2)[:n],
            "k2": np.tile(["a", "b"], n)[:n],
            "val": rng.normal(size=n),
            "name": rng.choice(["p", "q", "r"], n),
        }
    )
    shuffled = _shuffled(df, seed + 1)
    for key in (["k1", "k2"], ["k2", "k1"]):
        result = compare_rows_with_pk(df.lazy(), shuffled.lazy(), key)
        assert result.is_identical
        assert result.changed_row_count == 0
        assert result.column_changes == {}
        # ...and the same the other way around.
        assert compare_rows_with_pk(shuffled.lazy(), df.lazy(), key).is_identical


def test_compare_rows_with_pk_reordered_tables_still_show_real_changes():
    """Shuffling a table doesn't hide, or invent, differences.

    The right table is the left one reversed and with its columns reordered, but with the
    value of key 2 changed, key 3 gone and a new key 5. Exactly those are reported.
    """
    left = pl.DataFrame(
        {"id": [1, 2, 3, 4], "val": [1.0, 2.0, 3.0, 4.0], "name": list("abcd")}
    )
    # The same rows, reversed, with the columns reordered, except that row 2's value
    # changed, row 3 is gone, and there's a new row 5.
    right = pl.DataFrame(
        {"id": [5, 4, 2, 1], "val": [5.0, 4.0, 2.5, 1.0], "name": list("edba")}
    ).select("name", "val", "id")
    result = compare_rows_with_pk(left.lazy(), right.lazy(), ["id"])
    assert not result.is_identical
    assert result.column_changes == {"val": 1}
    assert result.changed_row_count == 1
    assert result.pk_diff.only_in_left.collect()["id"].to_list() == [3]
    assert result.pk_diff.only_in_right.collect()["id"].to_list() == [5]


def test_spill_dir_is_removed_by_cleanup_and_cleanup_can_repeat():
    """`cleanup()` deletes the directory, and calling it again is harmless."""
    spill_dir = SpillDir()
    path = Path(spill_dir.name)
    (path / "rows.parquet").write_bytes(b"rows")
    assert path.exists()

    spill_dir.cleanup()
    spill_dir.cleanup()

    assert not path.exists()


def test_spill_dir_is_removed_when_collected_without_a_warning():
    """A directory that is never cleaned up is deleted when collected, without a warning.

    Unlike `tempfile.TemporaryDirectory`, which warns with a `ResourceWarning`, leaving a
    `SpillDir` to be cleaned up implicitly is not a mistake. Warnings are made errors for
    the collection, so that any warning fails this.
    """
    spill_dir = SpillDir()
    path = Path(spill_dir.name)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        del spill_dir
        gc.collect()

    assert not path.exists()


def test_cleanup_deletes_the_files_behind_the_rows_of_a_diff():
    """`cleanup()` on a diff deletes the files that hold its rows, for either kind of diff."""
    row_set_diff = compare_rows_without_pk(
        pl.LazyFrame({"x": [1, 2, 3]}), pl.LazyFrame({"x": [3, 4]})
    )
    keyed_diff = compare_rows_with_pk(
        pl.LazyFrame({"id": [1, 2], "val": [1, 2]}),
        pl.LazyFrame({"id": [2, 3], "val": [20, 3]}),
        ["id"],
    )
    paths = [Path(row_set_diff.spill_dir.name), Path(keyed_diff.pk_diff.spill_dir.name)]
    assert all(path.exists() for path in paths)

    row_set_diff.cleanup()
    keyed_diff.cleanup()

    assert not any(path.exists() for path in paths)


@pytest.fixture
def tiny_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every table big enough to be compared in several partitions."""
    monkeypatch.setattr("pudl_diff.rows.MAX_ROWS_PER_PARTITION", 3)


def test_compare_rows_with_pk_partitioned_matches_unpartitioned(
    monkeypatch: pytest.MonkeyPatch,
):
    """Partitioning the key hashes doesn't change what a keyed comparison finds."""
    ids = list(range(40))
    left = pl.LazyFrame({"id": ids, "val": [float(i) for i in ids]})
    right = pl.LazyFrame(
        {
            "id": [*ids[:30], 100, 101],
            "val": [*(float(i) + (i % 7 == 0) for i in ids[:30]), 0.0, 0.0],
        }
    )
    expected = compare_rows_with_pk(left, right, ["id"])
    monkeypatch.setattr("pudl_diff.rows.MAX_ROWS_PER_PARTITION", 3)
    actual = compare_rows_with_pk(left, right, ["id"])
    assert (
        actual.pk_diff.only_in_left_count == expected.pk_diff.only_in_left_count == 10
    )
    assert (
        actual.pk_diff.only_in_right_count == expected.pk_diff.only_in_right_count == 2
    )
    assert actual.changed_row_count == expected.changed_row_count == 5
    assert actual.column_changes == expected.column_changes == {"val": 5}
    assert sorted(actual.pk_diff.only_in_left.collect()["id"]) == sorted(
        expected.pk_diff.only_in_left.collect()["id"]
    )


@pytest.mark.usefixtures("tiny_partitions")
def test_compare_rows_without_pk_partitioned_multiplicity():
    """Copies of a row are counted correctly across partitions."""
    left = pl.LazyFrame({"a": [1, 1, 1, 2, 3, 4, 5, 6]})
    right = pl.LazyFrame({"a": [1, 2, 2, 3, 7, 8]})
    result = compare_rows_without_pk(left, right)
    assert Counter(result.only_in_left.collect()["a"]) == Counter(
        {1: 2, 4: 1, 5: 1, 6: 1}
    )
    assert Counter(result.only_in_right.collect()["a"]) == Counter({2: 1, 7: 1, 8: 1})
    assert result.only_in_left_count == 5
    assert result.only_in_right_count == 3
    assert result.multiplicity_changed_row_count == 2
