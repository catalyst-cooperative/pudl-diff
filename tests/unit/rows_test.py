"""Unit tests for pudl_diff.rows."""

import gc
from collections import Counter
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pudl_diff.rows import (
    compare_rows_with_pk,
    compare_rows_without_pk,
)


def test_compare_rows_without_pk_identical():
    left = pl.LazyFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    right = pl.LazyFrame({"x": [3, 1, 2], "y": ["c", "a", "b"]})
    result = compare_rows_without_pk(left, right)
    assert result.is_identical
    assert result.only_in_left.collect().is_empty()
    assert result.only_in_right.collect().is_empty()


def test_compare_rows_without_pk_added_and_removed_rows():
    left = pl.LazyFrame({"x": [1, 2, 3]})
    right = pl.LazyFrame({"x": [1, 2, 4]})
    result = compare_rows_without_pk(left, right)
    assert not result.is_identical
    assert result.only_in_left.collect()["x"].to_list() == [3]
    assert result.only_in_right.collect()["x"].to_list() == [4]


def test_compare_rows_without_pk_changed_row():
    left = pl.LazyFrame({"x": [1, 2], "y": ["a", "b"]})
    right = pl.LazyFrame({"x": [1, 2], "y": ["a", "changed"]})
    result = compare_rows_without_pk(left, right)
    assert result.only_in_left.collect().to_dicts() == [{"x": 2, "y": "b"}]
    assert result.only_in_right.collect().to_dicts() == [{"x": 2, "y": "changed"}]


def test_compare_rows_without_pk_float_within_tolerance():
    left = pl.LazyFrame({"x": [1], "y": [1.00000001]})
    right = pl.LazyFrame({"x": [1], "y": [1.00000002]})
    assert compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_float_outside_tolerance():
    left = pl.LazyFrame({"x": [1], "y": [1.0]})
    right = pl.LazyFrame({"x": [1], "y": [2.0]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_exact_float_equality():
    left = pl.LazyFrame({"y": [1.00000001]})
    right = pl.LazyFrame({"y": [1.00000002]})
    result = compare_rows_without_pk(left, right, rtol=0, atol=0)
    assert not result.is_identical


def test_compare_rows_without_pk_same_sign_infinity():
    left = pl.LazyFrame({"y": [float("inf")]})
    right = pl.LazyFrame({"y": [float("inf")]})
    assert compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_opposite_sign_infinity():
    left = pl.LazyFrame({"y": [float("-inf")]})
    right = pl.LazyFrame({"y": [float("inf")]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_infinity_vs_finite():
    left = pl.LazyFrame({"y": [float("inf")]})
    right = pl.LazyFrame({"y": [1e10]})
    assert not compare_rows_without_pk(left, right).is_identical


def test_compare_rows_without_pk_counts_and_spill_cleanup():
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
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1, 2, 3]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2, 30, 4]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.pk_diff.only_in_left_count == 1
    assert result.pk_diff.only_in_right_count == 1
    assert result.changed_row_count == 1
    assert result.column_changes == {"val": 1}


def test_compare_rows_without_pk_multiplicity_change():
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
    left = pl.LazyFrame({"x": [5, 5, 1]})
    right = pl.LazyFrame({"x": [1]})
    result = compare_rows_without_pk(left, right)
    assert result.only_in_left.collect()["x"].to_list() == [5, 5]
    assert result.only_in_right_count == 0
    assert result.multiplicity_changed_row_count == 0


def test_compare_rows_without_pk_same_duplicates_are_identical():
    left = pl.LazyFrame({"x": [1, 1, 2], "y": ["a", "a", "b"]})
    right = pl.LazyFrame({"x": [2, 1, 1], "y": ["b", "a", "a"]})
    assert compare_rows_without_pk(left, right).is_identical


@pytest.mark.parametrize("seed", range(20))
def test_compare_rows_without_pk_matches_multiset_oracle(seed: int):
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
    left = pl.LazyFrame({"x": pl.Series([1, 2], dtype=pl.Int32)})
    right = pl.LazyFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
    with pytest.raises(pl.exceptions.SchemaError):
        compare_rows_without_pk(left, right)


def test_compare_rows_with_pk_mismatched_key_dtypes_raise():
    left = pl.LazyFrame(
        {"id": pl.Series([1, 2], dtype=pl.Int32), "val": [1, 2]},
    )
    right = pl.LazyFrame(
        {"id": pl.Series([1, 2], dtype=pl.Int64), "val": [1, 2]},
    )
    with pytest.raises(pl.exceptions.SchemaError):
        compare_rows_with_pk(left, right, ["id"])


def test_compare_rows_with_pk_tolerates_differing_non_key_dtypes():
    left = pl.LazyFrame({"id": [1, 2], "val": pl.Series([1, 2], dtype=pl.Int32)})
    right = pl.LazyFrame({"id": [1, 2], "val": pl.Series([1, 3], dtype=pl.Int64)})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}
    assert result.changed_row_count == 1


def test_compare_rows_with_pk_hash_semantics_match_equal_values():
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [0.0, float("nan"), None]})
    right = pl.LazyFrame({"id": [1, 2, 3], "val": [-0.0, -float("nan"), None]})
    assert compare_rows_with_pk(left, right, ["id"], rtol=0, atol=0).is_identical


def test_compare_rows_with_pk_duplicate_keys_are_counted():
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
    left = pl.LazyFrame({"id": [1, 2], "val": [1.0, 2.0], "name": ["a", "b"]})
    right = pl.LazyFrame({"id": [2, 1], "val": [2.0, 1.0], "name": ["b", "a"]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.pk_diff.is_identical
    assert result.column_changes == {}
    assert result.changed_row_count == 0


def test_compare_rows_with_pk_differing_key_sets():
    left = pl.LazyFrame({"id": [1, 2, 3], "val": [1.0, 2.0, 3.0]})
    right = pl.LazyFrame({"id": [2, 3, 4], "val": [2.0, 3.0, 4.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.pk_diff.only_in_left.collect()["id"].to_list() == [1]
    assert result.pk_diff.only_in_right.collect()["id"].to_list() == [4]
    assert result.column_changes == {}


def test_compare_rows_with_pk_left_only_and_right_only():
    left = pl.LazyFrame({"id": [1, 2]})
    right = pl.LazyFrame({"id": [1]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.pk_diff.only_in_left.collect()["id"].to_list() == [2]
    assert result.pk_diff.only_in_right.collect().is_empty()


def test_compare_rows_with_pk_numeric_column_mismatch():
    left = pl.LazyFrame({"id": [1, 2], "val": [1, 2]})
    right = pl.LazyFrame({"id": [1, 2], "val": [1, 99]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.pk_diff.is_identical
    assert result.column_changes == {"val": 1}
    assert result.changed_left.collect().to_dicts() == [{"id": 2, "val": 2}]
    assert result.changed_right.collect().to_dicts() == [{"id": 2, "val": 99}]


def test_compare_rows_with_pk_string_column_mismatch():
    left = pl.LazyFrame({"id": [1, 2], "name": ["a", "b"]})
    right = pl.LazyFrame({"id": [1, 2], "name": ["a", "changed"]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"name": 1}
    assert result.changed_left.collect().to_dicts() == [{"id": 2, "name": "b"}]
    assert result.changed_right.collect().to_dicts() == [{"id": 2, "name": "changed"}]


def test_compare_rows_with_pk_float_within_tolerance():
    left = pl.LazyFrame({"id": [1], "val": [1.00000001]})
    right = pl.LazyFrame({"id": [1], "val": [1.00000002]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.column_changes == {}


def test_compare_rows_with_pk_float_outside_tolerance():
    left = pl.LazyFrame({"id": [1], "val": [1.0]})
    right = pl.LazyFrame({"id": [1], "val": [2.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert not result.is_identical
    assert result.column_changes == {"val": 1}


def test_compare_rows_with_pk_same_sign_infinity():
    left = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    right = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical
    assert result.column_changes == {}


def test_compare_rows_with_pk_opposite_sign_infinity():
    left = pl.LazyFrame({"id": [1], "val": [float("-inf")]})
    right = pl.LazyFrame({"id": [1], "val": [float("inf")]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}


def test_compare_rows_with_pk_null_equality():
    left = pl.LazyFrame({"id": [1, 2], "val": [None, 1.0]})
    right = pl.LazyFrame({"id": [1, 2], "val": [None, 1.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.is_identical


def test_compare_rows_with_pk_null_vs_value_mismatch():
    left = pl.LazyFrame(
        {"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Float64}
    )
    right = pl.LazyFrame({"id": [1], "val": [1.0]})
    result = compare_rows_with_pk(left, right, ["id"])
    assert result.column_changes == {"val": 1}


def _shuffled(df: pl.DataFrame, seed: int) -> pl.DataFrame:
    """``df`` with its rows and its columns in a different, random order."""
    rng = np.random.default_rng(seed)
    columns = [df.columns[i] for i in rng.permutation(len(df.columns))]
    return df[rng.permutation(df.height)].select(columns)


@pytest.mark.parametrize("seed", range(10))
def test_compare_rows_without_pk_ignores_row_and_column_order(seed: int):
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
