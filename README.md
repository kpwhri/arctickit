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

df = pl.DataFrame({
    'region': ['north', 'north', 'south', 'south', 'south'],
    'status': ['open', 'closed', 'open', 'open', 'closed'],
})

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

df = pl.DataFrame({
    'name': ['Aino   ', 'Väinö   '],
})

clean_df = remove_nbsp(df)
print(clean_df)
```

#### `make_cast_to_date_expr(schema, date_col)`

Create a polars expression that converts a datetime or string column into a date column.

```python
import polars as pl

from arctickit.cast import make_cast_to_date_expr

df = pl.DataFrame({
        'event_date': ['2026-03-01', '2026-03-02'],
    })

expr = make_cast_to_date_expr(df.schema, 'event_date')
out_df = df.with_columns(expr)

print(out_df)
```


### `polars` expression helpers

These helpers build reusable Polars expressions for common row-wise and grouped flag logic. They are useful when a pipeline needs to repeatedly check completeness, combine several flag columns, or turn long classification records into wide indicator columns.

Because they return Polars expressions, they can be used inside `select`, `with_columns`, `filter`, and `group_by(...).agg(...)` without materializing intermediate data.

#### `all_columns_non_null_expr`

Return a Boolean expression that is `True` only when **all** named columns are non-null for a row.

This is useful for filtering records before downstream joins, classification, or scoring steps where incomplete rows should be excluded.

```python
import polars as pl

from arctickit.expr import all_columns_non_null_expr


df = pl.DataFrame({
        'mrn': ['A', 'B', 'C', 'D'],
        'encounter_id': [101, 102, None, 104],
        'diagnosis_code': ['E119', None, 'I10', 'J449'],
})

complete_df = df.filter(
    all_columns_non_null_expr('mrn', 'encounter_id', 'diagnosis_code')
)

print(complete_df)
```
Output:

```text
shape: (2, 3)
┌─────┬──────────────┬────────────────┐
│ mrn ┆ encounter_id ┆ diagnosis_code │
│ --- ┆ ---          ┆ ---            │
│ str ┆ i64          ┆ str            │
╞═════╪══════════════╪════════════════╡
│ A   ┆ 101          ┆ E119           │
│ D   ┆ 104          ┆ J449           │
└─────┴──────────────┴────────────────┘
```

#### `any_flag_column_true_expr`

Return a Boolean expression that is `True` when **any** named flag column is truthy for a row.

Each input column is cast to Boolean before being combined. This is useful when several detailed flags contribute to a broader derived flag.

```python
import polars as pl

from arctickit.expr import any_flag_column_true_expr


df = pl.DataFrame({
        'encounter_id': [1, 2, 3, 4],
        'CHF': [0, 0, 1, 0],
        'HTNWCHF': [0, 1, 0, 0],
        'HHRWCHF': [0, 0, 0, 0],
})

out_df = df.with_columns(
    any_flag_column_true_expr('CHF', 'HTNWCHF', 'HHRWCHF')
    .cast(pl.Int8)
    .alias('any_chf_related_flag')
)

print(out_df)
```

Output:

```text
shape: (4, 5)
┌──────────────┬─────┬─────────┬─────────┬──────────────────────┐
│ encounter_id ┆ CHF ┆ HTNWCHF ┆ HHRWCHF ┆ any_chf_related_flag │
│ ---          ┆ --- ┆ ---     ┆ ---     ┆ ---                  │
│ i64          ┆ i64 ┆ i64     ┆ i64     ┆ i8                   │
╞══════════════╪═════╪═════════╪═════════╪══════════════════════╡
│ 1            ┆ 0   ┆ 0       ┆ 0       ┆ 0                    │
│ 2            ┆ 0   ┆ 1       ┆ 0       ┆ 1                    │
│ 3            ┆ 1   ┆ 0       ┆ 0       ┆ 1                    │
│ 4            ┆ 0   ┆ 0       ┆ 0       ┆ 0                    │
└──────────────┴─────┴─────────┴─────────┴──────────────────────┘
```

#### `classification_indicator_exprs`

Return a list of aggregation expressions that create one indicator column for each expected classification value.

For each value, the generated expression checks whether that value appears anywhere in the current group. The result is cast to `Int8`, so each output column contains `1` when the value is present and `0` when it is absent.

This is useful for converting long classification data into one wide row of flags per entity, such as one row per encounter or one row per person.

```python
import polars as pl

from arctickit.expr import classification_indicator_exprs

df = pl.DataFrame({
        'encounter_id': [1, 1, 1, 2, 2, 3],
        'comorbidity': ['CHF', 'DM', 'CHF', 'RENLFAIL', 'DM', 'OTHER'],
})

flags_df = (
    df.group_by('encounter_id')
    .agg(
        classification_indicator_exprs(
            'comorbidity',
            ('CHF', 'DM', 'RENLFAIL'),
        )
    )
    .sort('encounter_id')
)

print(flags_df)
```

Output:

```text
shape: (3, 4)
┌──────────────┬─────┬─────┬──────────┐
│ encounter_id ┆ CHF ┆ DM  ┆ RENLFAIL │
│ ---          ┆ --- ┆ --- ┆ ---      │
│ i64          ┆ i8  ┆ i8  ┆ i8       │
╞══════════════╪═════╪═════╪══════════╡
│ 1            ┆ 1   ┆ 1   ┆ 0        │
│ 2            ┆ 0   ┆ 1   ┆ 1        │
│ 3            ┆ 0   ┆ 0   ┆ 0        │
└──────────────┴─────┴─────┴──────────┘
```

#### Combined example

The helpers can be combined in a pipeline: first remove incomplete records, then aggregate long classifications into wide flags, then derive broader summary flags.

```python
import polars as pl

from arctickit.expr import (
    all_columns_non_null_expr,
    any_flag_column_true_expr,
    classification_indicator_exprs,
)

df = pl.DataFrame({
    'encounter_id': [1, 1, 2, 2, 3, 4],
    'diagnosis_code': ['I50', 'E11', 'N18', None, 'E11', 'J44'],
    'comorbidity': ['CHF', 'DM', 'RENLFAIL', 'DM', 'DM', 'CHRNLUNG'],
})

out_df = (
    df.filter(
        all_columns_non_null_expr('encounter_id', 'diagnosis_code', 'comorbidity')
    )
    .group_by('encounter_id')
    .agg(
        classification_indicator_exprs(
            'comorbidity',
            ('CHF', 'DM', 'RENLFAIL', 'CHRNLUNG'),
        )
    )
    .with_columns(
        any_flag_column_true_expr('CHF', 'RENLFAIL')
        .cast(pl.Int8)
        .alias('major_comorbidity')
    )
    .sort('encounter_id')
)

print(out_df)
```

Output:

```text
shape: (4, 6)
┌──────────────┬─────┬─────┬──────────┬──────────┬───────────────────┐
│ encounter_id ┆ CHF ┆ DM  ┆ RENLFAIL ┆ CHRNLUNG ┆ major_comorbidity │
│ ---          ┆ --- ┆ --- ┆ ---      ┆ ---      ┆ ---               │
│ i64          ┆ i8  ┆ i8  ┆ i8       ┆ i8       ┆ i8                │
╞══════════════╪═════╪═════╪══════════╪══════════╪═══════════════════╡
│ 1            ┆ 1   ┆ 1   ┆ 0        ┆ 0        ┆ 1                 │
│ 2            ┆ 0   ┆ 0   ┆ 1        ┆ 0        ┆ 1                 │
│ 3            ┆ 0   ┆ 1   ┆ 0        ┆ 0        ┆ 0                 │
│ 4            ┆ 0   ┆ 0   ┆ 0        ┆ 1        ┆ 0                 │
└──────────────┴─────┴─────┴──────────┴──────────┴───────────────────┘
```



## Development

Run tests with:

```bash
pytest
# or
uv run pytest
```
