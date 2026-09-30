"""
Tests for groupby-like elements

* crosstab
"""
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from arctickit.groupby import crosstab


def test_crosstab_basic_count():
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y', None],
        'b': ['u', 'v', 'u', 'u', 'v', 'u'],
    })
    out = crosstab('a', 'b', data=df)
    # index column is named 'a'
    assert out.columns[0] == 'a'
    # columns u, v present
    assert set(out.columns[1:]) == {'u', 'v'}
    # counts: for a=x: u=1, v=1; a=y: u=2, v=1 (None is dropped by default)
    row_x = out.filter(pl.col('a') == 'x')
    row_y = out.filter(pl.col('a') == 'y')
    assert row_x.select('u').item() == 1
    assert row_x.select('v').item() == 1
    assert row_y.select('u').item() == 2
    assert row_y.select('v').item() == 1


def test_crosstab_with_values_sum_and_fill():
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
        'val': [1, 2, 3, 4, 5],
    })
    out = crosstab('a', 'b', values='val', aggfunc='sum', data=df, fill_value=0)
    # sums: x-u=1, x-v=2, y-u=7, y-v=5
    row_x = out.filter(pl.col('a') == 'x')
    row_y = out.filter(pl.col('a') == 'y')
    assert row_x.select('u').item() == 1
    assert row_x.select('v').item() == 2
    assert row_y.select('u').item() == 7
    assert row_y.select('v').item() == 5
    # fill produced zeros for missing combos
    assert out.null_count().sum_horizontal().sum() == 0


def test_crosstab_margins_counts():
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    out = crosstab('a', 'b', data=df, margins=True, margins_name='all')
    # check margins column and row
    assert 'all' in out.columns
    assert out.filter(pl.col('a') == 'all').shape[0] == 1
    # totals: u: 3, v: 2; row sums: x=2, y=3; grand total 5
    bottom = out.filter(pl.col('a') == 'all')
    assert bottom.select('u').item() == 3
    assert bottom.select('v').item() == 2
    # grand total in bottom-right
    assert bottom.select('all').item() == 5
    row_x = out.filter(pl.col('a') == 'x').select('all').item()
    row_y = out.filter(pl.col('a') == 'y').select('all').item()
    assert row_x == 2 and row_y == 3


def test_crosstab_normalize_all():
    """Test normalization over all cells, including proportional margins."""
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    exp_df = pl.DataFrame({
        'a': ['x', 'y', 'all'],
        'u': [1 / 5, 2 / 5, 3 / 5],
        'v': [1 / 5, 1 / 5, 2 / 5],
        'all': [2 / 5, 3 / 5, 1.0],
    })

    res_df = crosstab(
        'a', 'b', data=df, normalize=True,
        margins=True, margins_name='all', fill_value=0.0,
    )

    assert_frame_equal(res_df, exp_df, check_dtypes=False, check_exact=False)


def test_crosstab_normalize_index():
    """Test row normalization and the normalized overall column distribution."""
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    exp_df = pl.DataFrame({
        'a': ['x', 'y', 'all'],
        'u': [1 / 2, 2 / 3, 3 / 5],
        'v': [1 / 2, 1 / 3, 2 / 5],
    })

    res_df = crosstab('a', 'b', data=df, normalize='index', margins=True, margins_name='all')

    pl.testing.assert_frame_equal(
        res_df, exp_df, check_dtypes=False, check_exact=False,
    )


def test_crosstab_normalize_columns():
    """Test column normalization and the normalized overall row distribution."""
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    exp_df = pl.DataFrame({
        'a': ['x', 'y'],
        'u': [1 / 3, 2 / 3],
        'v': [1 / 2, 1 / 2],
        'all': [2 / 5, 3 / 5],
    })

    res_df = crosstab('a', 'b', data=df, normalize='columns', margins=True, margins_name='all')

    assert_frame_equal(res_df, exp_df, check_dtypes=False, check_exact=False)


def test_values_count():
    """Test that count with values counts only non-null values."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'A'],
        'category': ['x', 'x', 'y'],
        'value': [1, None, 3],
    })
    exp_df = pl.DataFrame({'group': ['A'], 'x': [1], 'y': [1]})

    res_df = crosstab(
        'group', 'category', values='value', data=df,
        aggfunc='count', fill_value=0,
    )

    assert_frame_equal(res_df, exp_df, check_dtypes=False)


def test_n_unique():
    """Test that n_unique counts distinct values within each cell."""
    df = pl.DataFrame({
        'group': ['A'] * 4,
        'category': ['x', 'x', 'x', 'y'],
        'value': [1, 1, 2, 10],
    })
    exp_df = pl.DataFrame({'group': ['A'], 'x': [2], 'y': [1]})

    res_df = crosstab('group', 'category', values='value', data=df, aggfunc='n_unique')

    assert_frame_equal(res_df, exp_df, check_dtypes=False)


def test_mean_row_margin():
    """Test that mean row margins aggregate original values, not cell means."""
    df = pl.DataFrame({
        'group': ['A'] * 3,
        'category': ['x', 'x', 'y'],
        'value': [1.0, 3.0, 100.0],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'All'],
        'x': [2.0, 2.0],
        'y': [100.0, 100.0],
        'All': [104 / 3, 104 / 3],
    })

    res_df = crosstab(
        'group', 'category', values='value', data=df,
        aggfunc='mean', margins=True, margins_name='All',
    )

    assert_frame_equal(res_df, exp_df, check_exact=False)


def test_mean_grand_margin():
    """Test that the grand mean margin aggregates all original observations."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'A', 'B'],
        'category': ['x', 'x', 'y', 'y'],
        'value': [1.0, 3.0, 100.0, 20.0],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'B', 'All'],
        'x': [2.0, None, 2.0],
        'y': [100.0, 20.0, 60.0],
        'All': [104 / 3, 20.0, 31.0],
    })

    res_df = crosstab(
        'group', 'category', values='value', data=df,
        aggfunc='mean', margins=True, margins_name='All',
    )

    assert_frame_equal(res_df, exp_df, check_exact=False)


def test_normalize_all_margins():
    """Test that normalize='all' produces proportional row and column margins."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'B', 'B'],
        'category': ['x', 'y', 'y', 'y'],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'B', 'All'],
        'x': [0.25, 0.0, 0.25],
        'y': [0.25, 0.5, 0.75],
        'All': [0.5, 0.5, 1.0],
    })

    res_df = crosstab(
        'group', 'category', data=df,
        normalize='all', margins=True,
        margins_name='All', fill_value=0,
    )

    assert_frame_equal(res_df, exp_df, check_exact=False)


def test_normalize_index_margins():
    """Test that normalize='index' adds a normalized margin row but no margin column."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'B', 'B'],
        'category': ['x', 'y', 'y', 'y'],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'B', 'All'],
        'x': [0.5, 0.0, 0.25],
        'y': [0.5, 1.0, 0.75],
    })

    res_df = crosstab(
        'group', 'category', data=df,
        normalize='index', margins=True,
        margins_name='All', fill_value=0,
    )

    assert_frame_equal(res_df, exp_df, check_exact=False)


def test_normalize_columns_margins():
    """Test that normalize='columns' adds a normalized margin column but no margin row."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'B', 'B'],
        'category': ['x', 'y', 'y', 'y'],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'B'],
        'x': [1.0, 0.0],
        'y': [1 / 3, 2 / 3],
        'All': [0.5, 0.5],
    })

    res_df = crosstab(
        'group', 'category', data=df,
        normalize='columns', margins=True,
        margins_name='All', fill_value=0,
    )

    assert_frame_equal(res_df, exp_df, check_exact=False)


def test_fill_value_preserves_null_index():
    """Test that fill_value does not replace a null index category."""
    df = pl.DataFrame(
        {'group': [None, 1], 'category': ['x', 'x']},
        schema={'group': pl.Int64, 'category': pl.String},
    )
    exp_df = pl.DataFrame(
        {'group': [None, 1], 'x': [1, 1]},
        schema={'group': pl.Int64, 'x': pl.Int64},
    )

    res_df = crosstab('group', 'category', data=df, dropna=False, fill_value=0)

    assert_frame_equal(res_df, exp_df, check_dtypes=False)


def test_missing_frequency_combination():
    """Test that unobserved frequency-table combinations are represented by zero."""
    df = pl.DataFrame({
        'group': ['A', 'A', 'B'],
        'category': ['x', 'y', 'y'],
    })
    exp_df = pl.DataFrame({
        'group': ['A', 'B'],
        'x': [1, 0],
        'y': [1, 1],
    })

    res_df = crosstab('group', 'category', data=df)

    assert_frame_equal(res_df, exp_df, check_dtypes=False)


def test_margin_name_collision():
    """Test that a margin name cannot overwrite an existing category."""
    df = pl.DataFrame({
        'group': ['A', 'B'],
        'category': ['All', 'x'],
    })

    with pytest.raises(ValueError, match='All'):
        crosstab('group', 'category', data=df, margins=True, margins_name='All')
