"""
Polars utilities.
"""
import polars as pl

from arctickit.types import PolarsFrame


def lowercase_columns(df: PolarsFrame):
    return df.rename({c: c.lower() for c in df.columns})


def remove_nbsp(df: PolarsFrame):
    return df.with_columns(
        [pl.col(pl.String).str.strip_chars_end()]
    )
