"""
Polars utilities.
"""
import polars as pl


def lowercase_columns(df):
    return df.rename({c: c.lower() for c in df.columns})


def remove_nbsp(df):
    return df.with_columns(
        [pl.col(pl.String).str.strip_chars_end()]
    )
