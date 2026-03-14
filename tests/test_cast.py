import datetime as dt

import polars as pl
import pytest

from arctickit.cast import make_cast_to_date_expr


def test_make_cast_to_date_expr_casts_utf8_to_date():
    df = pl.DataFrame(
        {
            'event_date': ['2026-03-01', '2026-03-02'],
        }
    )

    expr = make_cast_to_date_expr(df.schema, 'event_date')
    out_df = df.with_columns(expr)

    assert out_df.schema['event_date'] == pl.Date
    assert out_df['event_date'].to_list() == [dt.date(2026, 3, 1), dt.date(2026, 3, 2)]


def test_make_cast_to_date_expr_casts_datetime_to_date():
    df = pl.DataFrame(
        {
            'event_date': [
                dt.datetime(2026, 3, 1, 8, 30, 0),
                dt.datetime(2026, 3, 2, 17, 45, 0),
            ],
        }
    )

    expr = make_cast_to_date_expr(df.schema, 'event_date')
    out_df = df.with_columns(expr)

    assert out_df.schema['event_date'] == pl.Date
    assert out_df['event_date'].to_list() == [dt.date(2026, 3, 1), dt.date(2026, 3, 2)]


def test_make_cast_to_date_expr_returns_none_for_date_column():
    df = pl.DataFrame(
        {
            'event_date': [dt.date(2026, 3, 1), dt.date(2026, 3, 2)],
        }
    )

    expr = make_cast_to_date_expr(df.schema, 'event_date')

    assert expr is None


def test_make_cast_to_date_expr_raises_for_non_date_like_column():
    df = pl.DataFrame(
        {
            'event_date': [1, 2, 3],
        }
    )

    with pytest.raises(ValueError, match='Expected event_date to be pl.Date column'):
        make_cast_to_date_expr(df.schema, 'event_date')
