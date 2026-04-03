import polars as pl


def get_schema(df: pl.DataFrame | pl.LazyFrame):
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema()
    return df.schema


def get_columns(df: pl.DataFrame | pl.LazyFrame):
    return list(get_schema(df).names())


def ensure_lazy(df: pl.DataFrame | pl.LazyFrame) -> pl.LazyFrame:
    return df.lazy() if isinstance(df, pl.DataFrame) else df


def ensure_eager(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
    return df.collect() if isinstance(df, pl.LazyFrame) else df
