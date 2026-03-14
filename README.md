# arctickit

Small utilities for working with data in polars, with a focus on convenient SAS ingestion and a few dataframe helpers
that are handy in analysis workflows.

## Features

- Read SAS `sas7bdat` files into polars `DataFrame` objects
- Lazily scan SAS files into polars `LazyFrame` objects
- Read SAS metadata without loading the full dataset
- Print SAS metadata as a readable markdown-style report
- Create cross-tabulations with polars
- Apply a few lightweight dataframe cleanup helpers

## Installation

This project uses `uv`.

```bash
uv sync
```

## Quick start

```python
from pathlib import Path

from arctickit import read_sas, scan_sas

sas_path = Path('tests/data/airline.sas7bdat')

df = read_sas(sas_path)
print(df)

lf = scan_sas(sas_path)
print(lf.collect())
```

## API overview

### SAS readers

#### `read_sas(path, encoding='latin1')`

Read a SAS dataset into a polars `DataFrame`.

```python
from pathlib import Path

from arctickit import read_sas

sas_path = Path('tests/data/airline.sas7bdat')
df = read_sas(sas_path)

print(df.shape)
print(df.columns)
```

#### `scan_sas(path, engine='cpp', encoding='latin1')`

Create a lazy polars scan from a SAS dataset.

Supported engines:

- `'cpp'` - default
- `'readstat'`
- `'pio'`

```python
from pathlib import Path

import polars as pl

from arctickit import scan_sas

sas_path = Path('tests/data/airline.sas7bdat')

result_df = (
    scan_sas(sas_path, engine='cpp')
    .filter(pl.col('YEAR') >= 1950)
    .select('YEAR')
    .collect()
)

print(result_df)
```

### SAS metadata helpers

#### `read_sas_metadata(path, encoding='latin1')`

Read metadata from a SAS file without loading the whole dataset.

```python
from pathlib import Path

from arctickit.sas import read_sas_metadata

sas_path = Path('tests/data/airline.sas7bdat')
meta = read_sas_metadata(sas_path)

print(meta.number_rows)
print(meta.number_columns)
print(meta.column_names)
```

#### `print_sas_metadata(meta_or_path, encoding='latin1')`

Print a Markdown-style summary of dataset metadata and columns.

```python
from pathlib import Path

from arctickit.sas import print_sas_metadata

sas_path = Path('tests/data/airline.sas7bdat')
print_sas_metadata(sas_path)
```

### Crosstab helper

####
`crosstab(index, columns, values=None, *, data=None, aggfunc='count', normalize=None, margins=False, margins_name='all', dropna=True, fill_value=None)`

Create a cross-tabulation using polars data.

```python
import polars as pl

from arctickit.groupby import crosstab

df = pl.DataFrame(
    {
        'region': ['north', 'north', 'south', 'south', 'south'],
        'status': ['open', 'closed', 'open', 'open', 'closed'],
    }
)

out_df = crosstab(
    'region',
    'status',
    data=df,
    fill_value=0,
)

print(out_df)
```

### Utility helpers

#### `remove_nbsp(df)`

Apply string cleanup across string columns.

```python
import polars as pl

from arctickit import remove_nbsp

df = pl.DataFrame(
    {
        'name': ['Aino   ', 'Väinö   '],
    }
)

clean_df = remove_nbsp(df)
print(clean_df)
```

#### `make_cast_to_date_expr(schema, date_col)`

Create a polars expression that converts a datetime or string column into a date column.

```python
import polars as pl

from arctickit.cast import make_cast_to_date_expr

df = pl.DataFrame(
    {
        'event_date': ['2026-03-01', '2026-03-02'],
    }
)

expr = make_cast_to_date_expr(df.schema, 'event_date')
out_df = df.with_columns(expr)

print(out_df)
```

## Development

Run tests with:

```bash
pytest
# or
uv run pytest
```
