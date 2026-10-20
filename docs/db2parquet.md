---
title: Database queries to parquet
---

# Database queries to parquet

Save the results of a SQL query as **parquet files**, then explore those files with **polars** without running the SQL
again.

These helpers read database results in batches through a **sqlalchemy engine**. They can also filter rows, create new
columns, or combine records that belong together before writing the files.

## Contents

- [Choose a function](#choose-a-function)
- [Quick start: save and reuse a query](#quick-start-save-and-reuse-a-query)
- [Filter or modify data before saving](#filter-or-modify-data-before-saving)
- [Group notes and join their text](#group-notes-and-join-their-text)
- [Reuse, refresh, and open saved data](#reuse-refresh-and-open-saved-data)
- [Function reference](#function-reference)
- [Parameters and defaults](#parameters-and-defaults)
- [Practical tips](#practical-tips)

Back to [index](index.md)

## Choose a function

| Function                             | When to use it                                                                                                                               |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| **`get_or_create_parquet()`**        | **Recommended for most use cases.** Save a query if the output folder is new; otherwise, reuse its parquet files. It can also group records. |
| `read_database_to_parquet()`         | Always create a **new** dataset, optionally filtering or changing each batch first.                                                          |
| `read_grouped_database_to_parquet()` | Always create a **new** dataset, combining related rows (such as note fragments) across batch boundaries.                                    |

All three functions return a **polars `LazyFrame`**. A `LazyFrame` describes how to read and work with the saved data;
it does not immediately load all rows into memory. Call `.collect()` to get an in-memory `DataFrame` when needed.

## Quick start: save and reuse a query

First, create a sqlalchemy engine. This example uses a SQLite database named `example.db`; replace its connection URL
with the one for your database.

```python
from pathlib import Path
from sqlalchemy import create_engine

from arctickit import get_or_create_parquet

engine = create_engine('sqlite:///example.db')
path = Path('data/sales')

sales_df = get_or_create_parquet(
    query='SELECT sale_id, customer_id, amount FROM sales',
    engine=engine,
    output_dir=path,
)

# read only five rows from the saved data
print(sales_df.limit(5).collect())
```

**What happens:**

1. On the first call, the function runs the SQL query and saves the results to `data/sales/` as parquet files.
2. It returns a `LazyFrame` you can filter, summarize, or collect.
3. On later calls using the same output directory, it reuses the files **without running the query again**.

The output directory may contain several files:

```text
data/
└── sales/
    ├── part-00000000.parquet
    ├── part-00000001.parquet
    └── part-00000002.parquet
```

The number of files depends on the query results and batch size.

### Explore the saved data

```python
import polars as pl

high_value_sales_df = sales_df.filter(pl.col('amount') >= 100)
print(high_value_sales_df.collect())

# calculate one total per customer using the saved files
totals_df = sales_df.group_by('customer_id').agg(
    pl.col('amount').sum().alias('total_amount')
)
print(totals_df.collect())
```

**Why use this?** You can try different polars filters and calculations without waiting for the original database query
to run again. For large results, avoid collecting more rows than fit in memory.

## Filter or modify data before saving

If you already know which records you want, use `transform` to reduce the data **before** writing parquet.

```python
import polars as pl

from arctickit import read_database_to_parquet

completed_sales_df = read_database_to_parquet(
    query='SELECT customer_id, amount, status FROM sales',
    engine=engine,
    output_dir='data/completed_sales',
    transform=lambda df: (
        df.filter(pl.col('status') == 'complete')
        .with_columns((pl.col('amount') * 0.9).alias('discounted_amount'))
        .select('customer_id', 'amount', 'discounted_amount')
    ),
)

print(completed_sales_df.limit(5).collect())
```

The `transform` function receives one polars `DataFrame` at a time. Here it keeps completed sales, calculates a
discounted amount, and saves only three columns.

**Why use this?** Saving fewer rows or columns can reduce disk usage. On the other hand, if you expect to experiment
with many filters, save the unfiltered data first and filter its returned `LazyFrame` instead.

**Important:** `transform` runs **once per database batch**, not on the whole query. Row filters, column selection, and
row-by-row calculations are good choices. A global sort, deduplication, or grouping operation inside `transform` will
not see records in other batches.

## Group notes and join their text

Sometimes a single note is stored as several ordered rows. Suppose `note_parts` contains:

| note_id | part_number | note_text               |
|---------|------------:|-------------------------|
| 100     |           1 | `Called customer`       |
| 100     |           2 | `left a message`        |
| 101     |           1 | `Follow-up requested`   |
| 101     |           2 | `appointment scheduled` |

We want **one row per `note_id`**, with the `note_text` values joined using a comma, just like `','.join(...)` in
Python.

```python
import polars as pl
from arctickit import read_grouped_database_to_parquet

notes_df = read_grouped_database_to_parquet(
    query='''
        SELECT note_id, line, note_text
        FROM note_parts
        ORDER BY note_id, line
    ''',
    engine=engine,
    output_dir='data/notes',
    group_by='note_id',
    aggregations=[
        pl.col('note_text').str.join(',').alias('note_text'),
    ],
    batch_size=25_000,
)

print(notes_df.collect())
```

The saved result looks like this:

| note_id | note_text                                   |
|---------|---------------------------------------------|
| 100     | `Called customer,left a message`            |
| 101     | `Follow-up requested,appointment scheduled` |

The key expression is `pl.col('note_text').str.join(',')`: it combines the text values **within each `note_id` group**,
separated by commas.

### Why sorting matters

The SQL query must return all records for each `note_id` **together**. For text that must be joined in sequence, also
sort by the sequence column:

```sql
ORDER BY note_id, line
```

A group can begin in one database batch and end in another. The grouped function holds the unfinished group until all
its rows have arrived, then aggregates it once. It assumes the input is ordered; it does **not** sort the SQL results
for you.

You can aggregate more than one value at once:

```python
aggregations = [
    pl.col('note_text').str.join(',').alias('note_text'),
    pl.len().alias('part_count'),
]
```

To group by multiple columns, pass a list such as `group_by=['customer_id', 'note_id']` and order the SQL by both
grouping columns followed by the sequence column. If you use `transform` with grouped extraction, it must preserve the
grouping columns and the order and contiguity of the records.

**Memory note:** Grouped extraction processes batches, but it must retain an unfinished group. A very large group can
therefore use more memory than a single batch.

## Reuse, refresh, and open saved data

You can also combine **grouping and reuse** in `get_or_create_parquet()`:

```python
from arctickit import get_or_create_parquet

cached_notes_df = get_or_create_parquet(
    query='''
        SELECT note_id, part_number, note_text
        FROM note_parts
        ORDER BY note_id, part_number
    ''',
    engine=engine,
    output_dir='data/cached_notes',
    group_by='note_id',
    aggregations=[pl.col('note_text').str.join(',').alias('note_text')],
)
```

When both `group_by` and `aggregations` are supplied, `get_or_create_parquet()` uses grouped extraction for a new output
directory. Otherwise, it uses ordinary extraction.

**The directory is the cache key.** The function does not check whether your SQL, parameter values, transformations, or
database contents have changed. To get fresh results, choose a new output directory or remove the existing one
deliberately before rerunning.

To reopen files without using the original query at all:

```python
import polars as pl

saved_sales_df = pl.scan_parquet('data/sales/part-*.parquet')
print(saved_sales_df.limit(5).collect())
```

The two explicit `read_*_to_parquet()` functions are **create-only**: if the output path already exists, they raise
`FileExistsError` rather than overwriting its contents.

## Function reference

These short descriptions cover the main arguments, return values, and reasons to choose each function. All functions are
importable directly from `arctickit`.

### `get_or_create_parquet()`

```
get_or_create_parquet(
    query: str,
    engine: Engine,
    output_dir: str | Path,
    *,
    group_by: str | Sequence[str] | None = None,
    aggregations: Sequence[pl.Expr] | None = None,
    **kwargs,
) -> pl.LazyFrame
```

Retrieve an existing parquet dataset or create it from a database query if it does not exist.

This is the recommended function for routine use, particularly when the same database query may be needed across
multiple sessions.

#### Parameters

| Parameter             | Type                                             | Default   | Description                                                                                                                   |
|-----------------------|--------------------------------------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------|
| `query`               | `str`                                            | Required  | SQL query used to create the dataset if it does not already exist.                                                            |
| `engine`              | `sqlalchemy.Engine`                              | Required  | sqlalchemy engine used when a database extraction is required.                                                                |
| `output_dir`          | `str \| Path`                                    | Required  | Directory containing the cached parquet dataset, or the destination for a new dataset. Used as the cache identifier.          |
| `group_by`            | `str \| Sequence[str] \| None`                   | `None`    | Column or columns used to group database records. Must be supplied together with `aggregations`.                              |
| `aggregations`        | `Sequence[pl.Expr] \| None`                      | `None`    | Aggregation expressions applied to grouped records. Must be supplied together with `group_by`.                                |
| `transform`           | `Callable[[pl.DataFrame], pl.DataFrame] \| None` | `None`    | Optional function applied to each batch before writing. Useful for filtering, selecting columns, or creating derived columns. |
| `batch_size`          | `int`                                            | `100_000` | Number of rows requested per database batch. Smaller batches reduce memory use; larger batches may improve throughput.        |
| `compression`         | `str`                                            | `'zstd'`  | Compression codec used when writing parquet files.                                                                            |
| `schema_overrides`    | `dict \| None`                                   | `None`    | Mapping of column names to polars data types, used to override inferred database types.                                       |
| `infer_schema_length` | `int \| None`                                    | `100`     | Number of rows used for schema inference when applicable to the database driver.                                              |
| `execute_options`     | `dict \| None`                                   | `None`    | Additional options passed to `pl.read_database()`, such as SQL query parameters.                                              |

The final six parameters are passed through `**kwargs` to either `read_database_to_parquet()` or
`read_grouped_database_to_parquet()`.

#### Returns

`pl.LazyFrame` — A lazy view of an existing or newly created parquet dataset.

#### Example

```python
from arctickit import get_or_create_parquet

lf = get_or_create_parquet(
    query='SELECT * FROM transactions',
    engine=engine,
    output_dir='data/transactions',
)
df = lf.collect()
```

On the first call, the SQL query executes and its results are saved to parquet. Subsequent calls reuse the existing
files.

#### Example: Grouped extraction with caching

```python
import polars as pl

from arctickit import get_or_create_parquet

lf = get_or_create_parquet(
    query='''
          SELECT note_id, note_order, note_text       
           FROM notes        
           ORDER BY note_id, note_order    
           ''',
    engine=engine,
    output_dir='data/grouped_notes',
    group_by='note_id',
    aggregations=[
        pl.col('note_text').str.join(',').alias('note_text'),
    ],
)
```

#### Notes

- If a parquet dataset already exists at `output_dir`, the database is not queried.
- Cache validity is based on the output directory, not on the SQL query or transformation parameters.
- Changing the query or aggregation does not automatically refresh an existing dataset.
- To force a new extraction, use a different output directory or remove the existing dataset.

### `read_database_to_parquet`

Use this when you need a new export and want to filter or change data before storing it.

```text
read_database_to_parquet(
    query: str,
    engine: Engine,
    output_dir: str | Path,
    *,
    transform: FrameTransform | None = None,
    batch_size: int = 100_000,
    compression: str = 'zstd',
    schema_overrides: dict | None = None,
    infer_schema_length: int | None = 100,
    execute_options: dict | None = None,) -> pl.LazyFrame
```

Execute a SQL query in batches and save the results as parquet files. Optionally apply a polars transformation to each
batch before writing.

#### Parameters

| Parameter             | Type                                     | Default   | Description                                                                                                                                 |
|-----------------------|------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `query`               | `str`                                    | Required  | SQL query to execute.                                                                                                                       |
| `engine`              | `sqlalchemy.Engine`                      | Required  | sqlalchemy engine used to connect to the database. Connections are managed automatically, with streaming enabled where supported.           |
| `output_dir`          | `str \| Path`                            | Required  | Directory where parquet files will be written. Must not already exist.                                                                      |
| `transform`           | `Callable[[pl.DataFrame], pl.DataFrame]` | `None`    | Function applied to each batch before writing. Useful for filtering, selecting columns, or creating derived columns.                        |
| `batch_size`          | `int`                                    | `100_000` | Number of rows requested per database batch. Smaller batches reduce memory use; larger batches may improve throughput.                      |
| `compression`         | `str`                                    | `'zstd'`  | Compression codec used when writing parquet files.                                                                                          |
| `schema_overrides`    | `dict \| None`                           | `None`    | Mapping of column names to polars data types, used to override inferred database types.                                                     |
| `infer_schema_length` | `int \| None`                            | `100`     | Number of rows used for schema inference when applicable to the database driver. Set to `None` to inspect all available rows for inference. |
| `execute_options`     | `dict \| None`                           | `None`    | Additional options passed to `pl.read_database()`, such as SQL query parameters.                                                            |

#### Returns

`pl.LazyFrame` — A lazy view of the parquet files written to `output_dir`.

#### Example

```python
import polars as pl
from arctickit import read_database_to_parquet

lf = read_database_to_parquet(query='SELECT * FROM transactions', engine=engine, output_dir='data/transactions',
                              transform=lambda df: df.filter(pl.col('amount') > 100), )
df = lf.collect()
```

#### Notes

- Transformations operate on individual database batches, not the complete dataset.
- Existing output directories are not overwritten.
- The parquet dataset can be scanned again later without querying the database.

### `read_grouped_database_to_parquet()`

```text
read_grouped_database_to_parquet(
    query, engine, output_dir, *,
    group_by, aggregations, transform=None,
    batch_size=100_000, compression='zstd',
    schema_overrides=None, infer_schema_length=100, execute_options=None
) -> pl.LazyFrame
```

**Purpose:** Combine contiguous, already-ordered records into one row per group before saving.

- **`group_by`** — required column name or list of column names, such as `'note_id'`.
- **`aggregations`** — required list of polars expressions, such as `pl.col('note_text').str.join(',')`.
- **`transform`** — optional batch transformation that must preserve the grouping columns and group order.
- **Returns:** `pl.LazyFrame` scanning the aggregated parquet files.
- **Raises:** `FileExistsError` for an existing destination; `ValueError` for invalid grouping arguments or missing
  grouping columns, along with other extraction errors.

Use this for records like split notes or long text that may cross batch boundaries.

## Parameters and defaults

The following options are supported by both extraction functions. You can pass them to `get_or_create_parquet()` as well
when creating a new dataset.

| Parameter             | Default                         | What it does and when to change it                                                                                                    |
|-----------------------|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `query`               | Required                        | The SQL statement to run. Select only needed columns when possible.                                                                   |
| `engine`              | Required                        | A sqlalchemy `Engine`, usually made with `create_engine(...)`. The wrapper opens and closes connections for you.                      |
| `output_dir`          | Required                        | Folder containing the saved parquet files. Choose a new folder for a fresh extraction.                                                |
| `transform`           | `None`                          | Filter/select/calculate using polars before writing each batch.                                                                       |
| `batch_size`          | `100_000`                       | Requested rows per batch. Reduce it for memory-heavy rows, such as long text; increase it to reduce fetch overhead. Must be positive. |
| `compression`         | `'zstd'`                        | How parquet files are compressed. The default balances space and read/write speed.                                                    |
| `schema_overrides`    | `None`                          | Explicit polars column types when automatic type inference is unsuitable.                                                             |
| `infer_schema_length` | `100`                           | Number of rows to inspect for schema inference when supported by the database driver.                                                 |
| `execute_options`     | `None`                          | Additional database execution settings, commonly parameter values.                                                                    |
| `group_by`            | Required for grouped extraction | Column name or names identifying each group.                                                                                          |
| `aggregations`        | Required for grouped extraction | Expressions that produce the output columns for each group.                                                                           |

### Example: pass a SQL parameter

Use bound parameters instead of placing values directly into a SQL string:

```python
filtered_sales_df = get_or_create_parquet(
    query='SELECT * FROM sales WHERE amount >= :minimum_amount',
    engine=engine,
    output_dir='data/sales_minimum_250',
    execute_options={'parameters': {'minimum_amount': 250}},
)
```

Change `output_dir` when you change the parameter value; otherwise, the saved data will be reused even if the query
would now produce different results.

### Example: set a column type

```python
sales_df = read_database_to_parquet(
    query='SELECT sale_id, amount FROM sales',
    engine=engine,
    output_dir='data/typed_sales',
    schema_overrides={'sale_id': pl.Int64},
)
```

Use `schema_overrides` when a column is inferred with an unexpected type or types vary between batches.

## Practical tips

- **Use `get_or_create_parquet()` for repeatable analyses.** If the output directory exists, no new SQL query runs. The
  function does not automatically invalidate stale results.
- **Be mindful of memory.** The wrapper requests batches and sets sqlalchemy's `yield_per` option for streaming where
  the database driver supports it. Some drivers may still buffer rows internally. Grouped extraction can also retain a
  large unfinished group.
- **Keep batch-level work local.** Use `transform` for per-row filtering and changes, then use the saved `LazyFrame` for
  whole-dataset calculations.
- **Sort grouped SQL carefully.** Groups must be contiguous; for text fragments, sort by `note_id` and the fragment
  sequence column.
- **Expect create-only behavior from the two `read_*` functions.** Files are first written to a temporary directory and
  published when extraction finishes. A failed extraction cleans up staged files rather than leaving a partial published
  dataset.
- **Handle empty queries.** Empty batches with a known schema can produce an empty parquet dataset. If the reader yields
  no batches, it cannot determine the schema and raises an error.
