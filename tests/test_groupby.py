"""
Tests for groupby-like elements

* crosstab
"""
import polars as pl
import pytest

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
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    out = crosstab('a', 'b', data=df, normalize=True, margins=True, margins_name='all', fill_value=0.0)
    # counts grid: x-u=1, x-v=1, y-u=2, y-v=1
    expected = {
        ('x', 'u'): 1 / 5, ('x', 'v'): 1 / 5,
        ('y', 'u'): 2 / 5, ('y', 'v'): 1 / 5,
    }
    for a in ['x', 'y']:
        for b in ['u', 'v']:
            val = out.filter(pl.col('a') == a).select(b).item()
            assert pytest.approx(val, rel=1e-9) == expected[(a, b)]
    # margins should be 1 across normalized axis
    assert out.filter(pl.col('a') == 'all').select('u').item() == pytest.approx(1.0)
    assert out.filter(pl.col('a') == 'all').select('v').item() == pytest.approx(1.0)
    assert out.filter(pl.col('a') == 'x').select('all').item() == pytest.approx(1.0)
    assert out.filter(pl.col('a') == 'y').select('all').item() == pytest.approx(1.0)
    assert out.filter(pl.col('a') == 'all').select('all').item() == pytest.approx(1.0)


def test_crosstab_normalize_index_and_columns():
    margins_name = 'all'
    df = pl.DataFrame({
        'a': ['x', 'x', 'y', 'y', 'y'],
        'b': ['u', 'v', 'u', 'u', 'v'],
    })
    out_idx = crosstab('a', 'b', data=df, normalize='index', margins=True, margins_name=margins_name)
    # ensure ach row sums to ~1
    for a in ['x', 'y']:
        rowsum = out_idx.filter(pl.col('a') == a).select(pl.sum_horizontal(pl.all().exclude('a'))).item()
        assert pytest.approx(rowsum, rel=1e-9) == 1.0
    assert out_idx.filter(pl.col('a') == margins_name).select('u').item() == pytest.approx(1.0)
    assert out_idx.filter(pl.col('a') == margins_name).select('v').item() == pytest.approx(1.0)
    assert out_idx.filter(pl.col('a') == margins_name).select(margins_name).item() == pytest.approx(1.0)

    out_col = crosstab('a', 'b', data=df, normalize='columns', margins=True, margins_name=margins_name)
    # Each column sums to ~1, but we'll need to remove the final (summary) row
    col_sum_u = out_col.select(pl.sum('u')).item() - out_col.select('u').tail(1).item()
    col_sum_v = out_col.select(pl.sum('v')).item() - out_col.select('v').tail(1).item()
    assert pytest.approx(col_sum_u, rel=1e-9) == 1.0
    assert pytest.approx(col_sum_v, rel=1e-9) == 1.0
    # bottom row should be 1s and bottom-right 1.0
    assert out_col.filter(pl.col('a') == margins_name).select('u').item() == pytest.approx(1.0)
    assert out_col.filter(pl.col('a') == margins_name).select('v').item() == pytest.approx(1.0)
    assert out_col.filter(pl.col('a') == margins_name).select(margins_name).item() == pytest.approx(1.0)
