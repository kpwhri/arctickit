import polars as pl
import pytest

from arctickit.lazy import get_schema, ensure_lazy, ensure_eager


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        'a': [1, 2],
        'b': ['x', 'y'],
    })


@pytest.fixture
def sample_lf(sample_df):
    return sample_df.lazy()


def test_get_schema_dataframe(sample_df):
    schema = get_schema(sample_df)
    assert schema == sample_df.schema


def test_get_schema_lazyframe(sample_lf):
    schema = get_schema(sample_lf)
    assert schema == sample_lf.collect_schema()


def test_ensure_lazy_from_dataframe(sample_df):
    lf = ensure_lazy(sample_df)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().equals(sample_df)


def test_ensure_lazy_from_lazyframe(sample_lf):
    lf = ensure_lazy(sample_lf)
    assert lf is sample_lf  # should not create a new object


def test_ensure_eager_from_lazyframe(sample_lf):
    df = ensure_eager(sample_lf)
    assert isinstance(df, pl.DataFrame)
    assert df.equals(sample_lf.collect())


def test_ensure_eager_from_dataframe(sample_df):
    df = ensure_eager(sample_df)
    assert df is sample_df  # should not copy unnecessarily


def test_empty_dataframe():
    df = pl.DataFrame()
    lf = ensure_lazy(df)
    assert lf.collect().shape == (0, 0)


def test_schema_consistency(sample_df):
    lf = ensure_lazy(sample_df)
    assert get_schema(sample_df) == get_schema(lf)
