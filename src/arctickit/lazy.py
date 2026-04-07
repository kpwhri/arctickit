from typing import Literal

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


def ensure_mode(df: PolarsFrame, mode: Literal['lazy', 'eager']) -> PolarsFrame:
    if mode == 'lazy':
        return ensure_lazy(df)
    elif mode == 'eager':
        return ensure_eager(df)
    else:
        raise ValueError(f'Unrecognized mode "{mode}", expected "lazy" or "eager".')


def is_empty(df: PolarsFrame) -> bool:
    if isinstance(df, pl.LazyFrame):
        df = df.limit(1).collect()
    return df.is_empty()
