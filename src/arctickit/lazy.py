import polars as pl

from arctickit.types import PolarsFrame


def get_schema(df: PolarsFrame):
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema()
    return df.schema


def get_columns(df: PolarsFrame):
    return list(get_schema(df).names())


def ensure_lazy(df: PolarsFrame) -> pl.LazyFrame:
    return df.lazy() if isinstance(df, pl.DataFrame) else df


def ensure_eager(df: PolarsFrame) -> pl.DataFrame:
    return df.collect() if isinstance(df, pl.LazyFrame) else df
