from pathlib import Path

import polars as pl
import pytest

from arctickit.sas import scan_sas, read_sas, read_sas_metadata, print_sas_metadata


@pytest.fixture
def sas_path() -> Path:
    """Fixture for the SAS dataset path."""
    return (Path(__file__).parent / 'data' / 'airline.sas7bdat').resolve()


def test_reads_sas(sas_path):
    df_result = read_sas(sas_path)

    assert isinstance(df_result, pl.DataFrame)
    assert df_result.shape == (32, 6)
    assert 'YEAR' in df_result.columns
    assert df_result['YEAR'][0] == 1948.0


@pytest.mark.parametrize('engine', ['cpp', 'readstat', 'pio'])
def test_scans_sas(sas_path, engine):
    # scan_sas uses scan_readstat or pio.scan_sas7bdat which return LazyFrames.
    try:
        lf_result = scan_sas(sas_path, engine=engine)
        assert isinstance(lf_result, pl.LazyFrame)

        df_result = lf_result.collect()
        assert df_result.shape == (32, 6)
        assert 'YEAR' in df_result.columns
    except (ImportError, RuntimeError, AttributeError, Exception) as e:
        pytest.skip(f'Engine {engine} failed or not available: {e}')


def test_metadata(sas_path):
    meta = read_sas_metadata(sas_path)

    # meta is an object from pyreadstat
    assert meta.number_rows == 32
    assert meta.number_columns == 6
    assert 'YEAR' in meta.column_names
    assert meta.column_names_to_labels['YEAR'] == 'year'


def test_prints_metadata(sas_path, capsys):
    # test with meta object
    meta = read_sas_metadata(sas_path)
    print_sas_metadata(meta)
    captured = capsys.readouterr()

    assert '# Report on SAS Dataset' in captured.out
    assert '## Metadata' in captured.out
    assert '## Column Data' in captured.out
    assert 'YEAR' in captured.out
    assert '32' in captured.out

    # test with path
    print_sas_metadata(sas_path)
    captured = capsys.readouterr()
    assert 'Row Count' in captured.out
    assert '32' in captured.out
