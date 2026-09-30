Small utilities for working with data in polars, with a focus on convenient SAS ingestion and a few dataframe helpers
that are handy in analysis workflows.

## Features

- Read SAS `sas7bdat` files into polars `DataFrame` objects
- Lazily scan SAS files into polars `LazyFrame` objects
- Read SAS metadata without loading the full dataset
- Print SAS metadata as a readable markdown-style report
- Create cross-tabulations with polars
- Apply a few lightweight dataframe cleanup helpers

## Contents

- [API](api.md)
- [crosstab](crosstab.md)

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