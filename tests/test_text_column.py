"""Tests for the TextColumn expression helper."""

from dataclasses import FrozenInstanceError

import polars as pl
import pytest

from arctickit import TextColumn


@pytest.fixture
def df() -> pl.DataFrame:
    """Include exact matches, partial matches, metacharacters, and null."""
    return pl.DataFrame({'code': [
        'AB12', 'AB99', 'CD12', 'xAB12', 'ab12', 'file.txt',
        'file.csv', 'a.b', 'axb', '', 'AB12\nCD34', None,
    ]})


def matches(df: pl.DataFrame, expr: pl.Expr) -> list[bool | None]:
    """Evaluate one predicate without dropping null results via filter."""
    assert isinstance(expr, pl.Expr)
    return df.select(expr.alias('matched'))['matched'].to_list()


@pytest.mark.parametrize(('method', 'argument', 'matched_rows'), [
    ('starts_with', 'AB', {0, 1, 10}),
    ('starts_with_any', ['AB', 'CD'], {0, 1, 2, 10}),
    ('ends_with', '12', {0, 2, 3, 4}),
    ('ends_with_any', ['.txt', '.csv'], {5, 6}),
    ('equals', 'AB12', {0}),
    ('not_equals', 'AB12', set(range(1, 11))),
    ('is_in', ['AB12', 'CD12'], {0, 2}),
    ('not_in', ['AB12', 'CD12'], set(range(11)) - {0, 2}),
    ('contains', 'a.b', {7}),
    ('contains_any', ['.txt', 'CD'], {2, 5, 10}),
    ('search', r'AB\d{2}', {0, 1, 3, 10}),
    ('isearch', r'^AB\d{2}', {0, 1, 4, 10}),
    ('fullmatch', r'AB\d{2}', {0, 1}),
])
def test_predicates(df, method, argument, matched_rows):
    """Check actual matches, including a final null input."""
    result = matches(df, getattr(TextColumn('code'), method)(argument))
    assert result == [i in matched_rows for i in range(11)] + [None]


@pytest.mark.parametrize(('short', 'long', 'argument'), [
    ('sw', 'starts_with', 'AB'),
    ('swany', 'starts_with_any', ('AB', 'CD')),
    ('ew', 'ends_with', '12'),
    ('ewany', 'ends_with_any', ('.txt', '.csv')),
    ('eq', 'equals', 'AB12'),
    ('ne', 'not_equals', 'AB12'),
    ('eqany', 'is_in', ('AB12', 'CD12')),
])
def test_short_names_are_equivalent(df, short, long, argument):
    """Test each abbreviated method against its descriptive counterpart."""
    column = TextColumn('code')
    assert matches(df, getattr(column, short)(argument)) == matches(
        df, getattr(column, long)(argument),
    )
    assert long in getattr(column, short).__doc__


@pytest.mark.parametrize('method', ['starts_with_any', 'ends_with_any'])
def test_empty_prefix_and_suffix_sequences(df, method):
    """Empty collections match no non-null strings; null remains null."""
    result = matches(df, getattr(TextColumn('code'), method)([]))
    # n.b., `starts_with_any` will fail due to incorrect polars_cnlp implementation
    assert result == [False] * 11 + [None]


@pytest.mark.parametrize('method', ['starts_with_any', 'ends_with_any'])
def test_empty_string_in_prefix_and_suffix_sequences(df, method):
    """An empty literal is a prefix/suffix of every non-null string."""
    result = matches(df, getattr(TextColumn('code'), method)(['']))
    assert result == [True] * 11 + [None]


def test_fullmatch_uses_absolute_anchors():
    """Inline multiline mode must not permit matching just one line."""
    frame = pl.DataFrame({'code': ['AB12', 'AB12\n', 'AB12\nCD34', None]})
    result = matches(frame, TextColumn('code').fullmatch(r'(?m:AB\d{2})'))
    assert result == [True, False, False, None]


def test_contains_literal_regex_metacharacters(df):
    """Contains a dot literally, while regex search treats it as a wildcard."""
    column = TextColumn('code')
    assert matches(df, column.contains('a.b'))[8] is False
    assert matches(df, column.search('a.b'))[8] is True


def test_operators_composable_in_lazy_filter(df):
    """Check expressions can be combined and used without eager UDFs."""
    code = TextColumn('code')
    result = df.lazy().filter(
        (code.sw('AB') | code.eq('CD12')) & ~code.ew('99'),
    ).collect()
    assert result['code'].to_list() == ['AB12', 'CD12', 'AB12\nCD34']


def test_frozen_and_column_specific():
    """Column name is immutable and the expression references that column."""
    column = TextColumn('description')
    frame = pl.DataFrame({'code': ['AB'], 'description': ['AB12']})
    assert matches(frame, column.sw('AB1')) == [True]
    assert isinstance(column.expr, pl.Expr)
    with pytest.raises(FrozenInstanceError):
        column.name = 'code'


def test_invalid_regex_raises_polars_error():
    """Do not silently reinterpret malformed regex patterns as literals."""
    with pytest.raises(pl.exceptions.ComputeError):
        pl.DataFrame({'code': ['abc']}).select(TextColumn('code').search('('))
