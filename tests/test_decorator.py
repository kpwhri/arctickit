import polars as pl

from arctickit.decorator import requires_eager, requires_lazy


def test_requires_eager():
    @requires_eager
    def summarize(df: pl.DataFrame) -> pl.DataFrame:
        assert isinstance(df, pl.DataFrame)
        return df.select(
            pl.col('col1').sum().alias('col1_sum'),
            pl.col('col2').mean().alias('col2_mean'),
        )

    df = pl.DataFrame({'col1': [1, 2, 3], 'col2': [10, 20, 30]})
    lazy_df = df.lazy()

    result_eager = summarize(df)  # input eager → returns eager
    result_lazy = summarize(lazy_df)  # input lazy → collected internally → returns eager

    assert isinstance(result_eager, pl.DataFrame), 'Eager input should return DataFrame'
    assert isinstance(result_lazy, pl.LazyFrame), 'Lazy input should return LazyFrame'

    exp_df = pl.DataFrame({
        'col1_sum': [6],
        'col2_mean': [20.0],
    })

    assert result_eager.equals(exp_df), 'Eager result mismatch'
    assert result_lazy.collect().equals(exp_df), 'Lazy result mismatch'


def test_requires_lazy():
    @requires_lazy
    def summarize(lf: pl.LazyFrame) -> pl.LazyFrame:
        assert isinstance(lf, pl.LazyFrame)
        return lf.select([
            pl.col('col1').sum().alias('col1_sum'),
            pl.col('col2').mean().alias('col2_mean')
        ])

    df = pl.DataFrame({'col1': [1, 2, 3], 'col2': [10, 20, 30]})
    lazy_df = df.lazy()

    result_eager = summarize(df)  # input eager → returns eager
    result_lazy = summarize(lazy_df)  # input lazy → collected internally → returns eager

    assert isinstance(result_eager, pl.DataFrame), 'Eager input should return DataFrame'
    assert isinstance(result_lazy, pl.LazyFrame), 'Lazy input should return LazyFrame'

    exp_df = pl.DataFrame({
        'col1_sum': [6],
        'col2_mean': [20.0],
    })

    assert result_eager.equals(exp_df), 'Eager result mismatch'
    assert result_lazy.collect().equals(exp_df), 'Lazy result mismatch'
