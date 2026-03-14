from pathlib import Path
from typing import Literal

import polars as pl
import pyreadstat
import polars_io as pio
from polars_readstat import scan_readstat
from tabulate import tabulate


def scan_sas(path: Path | str, engine: Literal['readstat', 'cpp', 'pio'] = 'cpp', encoding='latin1') -> pl.LazyFrame:
    """Create a polars LazyFrame from a sas7bdat file.

    :return:

    Args:
        path: path to sas7bdat file
        engine: 'readstat', 'cpp' for faster, 'pio' for more manual
        encoding:
    """
    match engine:
        case 'pio':
            return pio.scan_sas7bdat(path, encoding=encoding)
        case 'readstat':
            return scan_readstat(str(path), engine=engine)
        case 'cpp':
            return scan_readstat(str(path), engine=engine)
        case _:
            return scan_readstat(str(path), engine='cpp')


def read_sas(path: Path | str, encoding='latin1') -> pl.DataFrame:
    """Create a polars DataFrame from a sas7bdat file.

    Parameters:
        path: Path to sas7bdat file
        encoding: defaults to 'latin1', but 'utf8' might be useful
    """
    df, meta = pyreadstat.read_sas7bdat(path, output_format='polars', encoding=encoding)
    return df


def read_sas_metadata(path: Path | str, encoding='latin1'):
    """Just read metadata from SAS file.

    Parameters:
        path:
        encoding:

    Returns:

    """
    _, meta = pyreadstat.read_sas7bdat(path, output_format='polars', encoding=encoding, metadataonly=True)
    return meta


def print_sas_metadata(meta, encoding='latin1'):
    """

    Parameters:
        meta: meta object from pyreadstat.read_sas7bdat or Path to sas7bdat file
        encoding: only used if a path is set, defaults to latin1

    Returns:

    """
    if isinstance(meta, (Path, str)):
        meta = read_sas_metadata(meta, encoding=encoding)

    print(f'# Report on SAS Dataset')
    print('\n## Metadata')
    print(tabulate([
        ['Row Count', f'{meta.number_rows:,}'],
        ['Column Count', f'{meta.number_columns:,}'],
        ['File Encoding', meta.file_encoding or 'Unspecified'],
        ['Creation Date', f'{getattr(meta, "creation_time", "Unspecified")}'],
        ['Modified Date', f'{getattr(meta, "modification_time", "Unspecified")}'],
    ],
        headers=['Metric', 'Value'],
        tablefmt='pipe',
    ))

    table_data = []
    for col in meta.column_names:
        label = meta.column_names_to_labels.get(col, '')
        var_type = meta.readstat_variable_types.get(col, '')
        sas_format = meta.original_variable_types.get(col, '')

        value_labels = meta.variable_value_labels.get(col, {})
        label_count = len(value_labels) if value_labels else 0

        table_data.append([
            col,
            var_type,
            sas_format,
            label,
            label_count
        ])

    print('\n## Column Data')
    print(tabulate(
        table_data,
        headers=['Name', 'Type', 'SAS Format', 'Label', 'Value Labels'],
        tablefmt='pipe',
        colalign=('left', 'center', 'center', 'left', 'center')
    ))
