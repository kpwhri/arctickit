import datetime as dt

import polars as pl
import pytest

from arctickit.cast import make_cast_to_date_expr, to_date_lit_expr, to_date_expr


def test_make_cast_to_date_expr_casts_utf8_to_date():
    df = pl.DataFrame({'event_date': ['2026-03-01', '2026-03-02']})

    expr = make_cast_to_date_expr(df.schema, 'event_date')
    out_df = df.with_columns(expr)

    assert out_df.schema['event_date'] == pl.Date
    assert out_df['event_date'].to_list() == [dt.date(2026, 3, 1), dt.date(2026, 3, 2)]


def test_make_cast_to_date_expr_casts_datetime_to_date():
    df = pl.DataFrame({'event_date': [
        dt.datetime(2026, 3, 1, 8, 30, 0),
        dt.datetime(2026, 3, 2, 17, 45, 0),
    ]})

    expr = make_cast_to_date_expr(df.schema, 'event_date')
    out_df = df.with_columns(expr)

    assert out_df.schema['event_date'] == pl.Date
    assert out_df['event_date'].to_list() == [dt.date(2026, 3, 1), dt.date(2026, 3, 2)]


def test_make_cast_to_date_expr_returns_none_for_date_column():
    df = pl.DataFrame({'event_date': [
        dt.date(2026, 3, 1), dt.date(2026, 3, 2)
    ]})

    expr = make_cast_to_date_expr(df.schema, 'event_date')
    out_df = df.with_columns(expr)

    assert out_df.schema['event_date'] == pl.Date


def test_make_cast_to_date_expr_raises_for_non_date_like_column():
    df = pl.DataFrame(
        {
            'event_date': [1, 2, 3],
        }
    )

    with pytest.raises(TypeError, match='Unsupported date column dtype'):
        make_cast_to_date_expr(df.schema, 'event_date')


def test_to_date_lit_expr_custom_format():
    result = pl.select(to_date_lit_expr('11/13/2026', 'result', format='%m/%d/%Y'))

    assert result['result'].to_list() == [dt.date(2026, 11, 13)]


def test_to_date_lit_expr_invalid_non_strict():
    result = pl.select(to_date_lit_expr('not-a-date', 'result', strict=False))

    assert result.schema['result'] == pl.Date
    assert result['result'].to_list() == [None]


def test_to_date_lit_expr_strict_raises():
    with pytest.raises(pl.exceptions.PolarsError):
        pl.select(to_date_lit_expr('not-a-date', 'result'))


@pytest.mark.parametrize('value', [123, True, 3.14, []])
def test_to_date_lit_expr_unsupported_type(value):
    with pytest.raises(TypeError, match='Unsupported date literal type'):
        to_date_lit_expr(value, 'result')


def test_to_date_expr_accepts_plain_mapping():
    df = pl.DataFrame({'input': ['2026-11-13']})
    result = df.select(to_date_expr({'input': pl.String}, 'input'))

    assert result.schema['input'] == pl.Date
    assert result['input'].to_list() == [dt.date(2026, 11, 13)]


def test_to_date_expr_missing_column():
    with pytest.raises(KeyError):
        to_date_expr({'other': pl.String}, 'input')
