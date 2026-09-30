# Crosstabs

`crosstab` creates a cross-tabulation of two categorical factors using polars.

It can be used for:

- simple frequency tables,
- aggregation of a third value column,
- row-, column-, or globally normalized tables,
- marginal totals or aggregate margins,
- null-category handling, and
- filling missing combinations.

The function is conceptually similar to `pandas.crosstab`, but returns a standard polars `DataFrame` and uses polars
aggregation semantics. Unlike polars, it is limited to two categorical factors (see [Limitations](#limitations)). For this see the section on how to use `pandas`.

## Contents

## Contents

- [Usage](#basic-usage)
- [Parameters](#parameters)
- [Frequency Tables](#frequency-tables)
- [Value Aggregation](#aggregating-values)
- [Normalization](#normalization)
- [Margins](#margins)
- [Missing Values](#missing-values)
- [Examples](#examples)
- [Implementation Notes](#implementation-notes)
- [Comparison With `pivot`](#comparison-with-pivot)
- [Limitations (i.e., when to use pandas)](#limitations)

## Basic Usage

```python
import polars as pl

df = pl.DataFrame({
    'group': ['A', 'A', 'B', 'B', 'B'],
    'category': ['x', 'y', 'x', 'x', 'y'],
})

crosstab('group', 'category', data=df)
```

Result:

```text
shape: (2, 3)
┌───────┬─────┬─────┐
│ group ┆ x   ┆ y   │
│ ---   ┆ --- ┆ --- │
│ str   ┆ u32 ┆ u32 │
╞═══════╪═════╪═════╡
│ A     ┆ 1   ┆ 1   │
│ B     ┆ 2   ┆ 1   │
└───────┴─────┴─────┘
```

The first argument determines the rows of the table, while the second determines the generated columns.

## Function Signature

```python
def crosstab(
        index: str | pl.Series | Sequence[Any],
        columns: str | pl.Series | Sequence[Any],
        values: str | pl.Series | Sequence[Any] | None = None,
        *,
        data: pl.DataFrame | None = None,
        aggfunc: AggFunc = 'count',
        normalize: Normalize = None,
        margins: bool = False,
        margins_name: str = 'all',
        dropna: bool = True,
        fill_value: Any | None = None,
) -> pl.DataFrame:
    ...
```

## Parameters

### `index`

The factor used to create rows.

A string is interpreted as a column name in `data`:

```python
crosstab('group', 'category', data=df)
```

A polars `Series` may also be supplied directly:

```python
crosstab(df['group'], df['category'])
```

A sequence can also be used:

```python
crosstab(
    ['A', 'A', 'B'],
    ['x', 'y', 'x'],
)
```

A sequence represents the observations for a single factor. It does not represent multiple index columns.

---

### `columns`

The factor whose unique values become columns in the resulting table.

For example:

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'B'],
    'status': ['yes', 'no', 'yes'],
})

crosstab('group', 'status', data=df)
```

produces columns corresponding to `no` and `yes`.

---

### `values`

Optional values to aggregate within each `(index, columns)` combination.

When `values` is omitted, `crosstab` creates a frequency table:

```python
crosstab('group', 'category', data=df)
```

When `values` is supplied, `aggfunc` determines how those values are summarized:

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='mean',
)
```

For example, given:

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'A', 'B'],
    'category': ['x', 'x', 'y', 'y'],
    'score': [1.0, 3.0, 100.0, 20.0],
})
```

then:

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='mean',
)
```

produces conceptually:

```text
group      x       y
A        2.0   100.0
B       null    20.0
```

---

### `data`

Optional polars `DataFrame` used to resolve string column names.

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
)
```

If `index`, `columns`, or `values` are passed as strings, `data` must be provided.

---

### `aggfunc`

Determines how `values` are aggregated within each cell.

Supported values are:

```text
count
sum
mean
min
max
median
first
last
n_unique
```

Example:

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='median',
)
```

When `values=None`, the function produces frequency counts regardless of `aggfunc`.

#### `count`

With a `values` column, `count` counts non-null values:

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'A'],
    'category': ['x', 'x', 'y'],
    'value': [1, None, 3],
})
```

```python
crosstab(
    'group',
    'category',
    values='value',
    data=df,
    aggfunc='count',
)
```

results in:

```text
group   x   y
A       1   1
```

This differs from a frequency table, where every observation contributes to the count.

#### `n_unique`

`n_unique` follows polars semantics, including polars' treatment of null as a unique value.

---

## Frequency Tables

The simplest form is:

```python
crosstab(
    'group',
    'category',
    data=df,
)
```

Each cell contains the number of observations belonging to that combination.

Given:

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'B'],
    'category': ['x', 'y', 'y'],
})
```

the result is:

```text
group   x   y
A       1   1
B       0   1
```

For frequency tables, missing combinations are represented as `0` rather than `null`.

---

## Aggregating Values

A third variable can be summarized instead of counting observations.

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'A', 'B'],
    'category': ['x', 'x', 'y', 'y'],
    'value': [1, 3, 100, 20],
})
```

Mean values:

```python
crosstab(
    'group',
    'category',
    values='value',
    data=df,
    aggfunc='mean',
)
```

Result:

```text
group      x       y
A        2.0   100.0
B       null    20.0
```

Unlike frequency tables, missing combinations remain null unless `fill_value` is supplied.

For example:

```python
crosstab(
    'group',
    'category',
    values='value',
    data=df,
    aggfunc='mean',
    fill_value=0,
)
```

produces:

```text
group     x       y
A       2.0   100.0
B       0.0    20.0
```

---

# Normalization

`normalize` converts the table to proportions.

Supported values are:

```text
None
True
'all'
'index'
'columns'
```

`True` is equivalent to `'all'`.

## Normalize Over the Entire Table

```python
crosstab(
    'a',
    'b',
    data=df,
    normalize='all',
)
```

Suppose the raw counts are:

```text
       u   v
x      1   1
y      2   1
```

There are five observations in total.

With:

```python
normalize = 'all'
```

each cell is divided by `5`:

```text
       u     v
x     .2    .2
y     .4    .2
```

All cells together sum to `1.0`.

`normalize=True` is an alias:

```python
crosstab(
    'a',
    'b',
    data=df,
    normalize=True,
)
```

---

## Normalize Within Rows

```python
crosstab(
    'a',
    'b',
    data=df,
    normalize='index',
)
```

Each row is divided by its own total.

For:

```text
       u   v
x      1   1
y      2   1
```

the result is:

```text
       u       v
x     .5      .5
y     .667    .333
```

Each row therefore sums to `1.0`.

This is useful when asking questions such as:

> Within each group, what proportion belongs to each category?

---

## Normalize Within Columns

```python
crosstab(
    'a',
    'b',
    data=df,
    normalize='columns',
)
```

Each column is divided by its own total.

Given:

```text
       u   v
x      1   1
y      2   1
```

the result is:

```text
       u       v
x     .333    .5
y     .667    .5
```

Each generated column sums to `1.0`.

This is useful when asking:

> Within each category, what proportion belongs to each group?

---

# Margins

Set:

```python
margins = True
```

to add aggregate margins.

The label is controlled using:

```python
margins_name = 'all'
```

For a basic frequency table:

```python
crosstab(
    'a',
    'b',
    data=df,
    margins=True,
)
```

a table such as:

```text
       u   v
x      1   1
y      2   1
```

becomes:

```text
       u   v   all
x      1   1     2
y      2   1     3
all    3   2     5
```

The right margin contains row totals.

The bottom margin contains column totals.

The bottom-right value is the grand total.

---

## Margins With Aggregations

Margins are computed directly from the original observations.

This distinction is important for non-additive aggregations such as:

- `mean`
- `median`
- `n_unique`

Consider:

```python
df = pl.DataFrame({
    'group': ['A', 'A', 'A'],
    'category': ['x', 'x', 'y'],
    'value': [1.0, 3.0, 100.0],
})
```

The cell means are:

```text
x = mean(1, 3) = 2
y = mean(100)  = 100
```

The row margin is **not**:

```text
2 + 100 = 102
```

Instead, the original observations are aggregated again:

```text
mean(1, 3, 100) = 34.67
```

Therefore:

```python
crosstab(
    'group',
    'category',
    values='value',
    data=df,
    aggfunc='mean',
    margins=True,
)
```

produces conceptually:

```text
group     x       y       all
A        2.0   100.0    34.67
all      2.0   100.0    34.67
```

This behavior ensures that margins remain mathematically meaningful for non-additive aggregations.

---

# Normalized Margins

Margins are also normalized when `normalize` is used.

This is different from simply placing `1.0` in every margin cell.

Suppose:

```python
df = pl.DataFrame({
    'a': ['x', 'x', 'y', 'y', 'y'],
    'b': ['u', 'v', 'u', 'u', 'v'],
})
```

The raw counts are:

```text
       u   v   all
x      1   1     2
y      2   1     3
all    3   2     5
```

## `normalize='all'`

The entire table is expressed relative to the grand total:

```text
       u     v    all
x     .2    .2    .4
y     .4    .2    .6
all   .6    .4   1.0
```

The bottom margin contains the overall column proportions:

```text
u = 3 / 5
v = 2 / 5
```

The right margin contains the overall row proportions:

```text
x = 2 / 5
y = 3 / 5
```

Only the grand-total cell is necessarily `1.0`.

---

## `normalize='index'`

Each regular row is normalized independently:

```text
       u       v
x     .5      .5
y     .667    .333
```

When margins are enabled, an overall normalized row is appended:

```text
       u       v
x     .5      .5
y     .667    .333
all   .6      .4
```

The `all` row describes the overall distribution of the column factor.

There is no additional `all` column in this mode.

---

## `normalize='columns'`

Each regular column is normalized independently:

```text
       u       v
x     .333    .5
y     .667    .5
```

With margins enabled, the overall row distribution is added as an `all` column:

```text
       u       v    all
x     .333    .5    .4
y     .667    .5    .6
```

There is no additional `all` row in this mode.

---

## Normalization Summary

| `normalize` | Cell normalization    | Bottom margin              | Right margin            |
|-------------|-----------------------|----------------------------|-------------------------|
| `None`      | None                  | aggregate by column        | aggregate by row        |
| `'all'`     | divide by grand total | overall column proportions | overall row proportions |
| `'index'`   | normalize each row    | overall column proportions | none                    |
| `'columns'` | normalize each column | none                       | overall row proportions |

---

# Missing Values

## Factor Nulls

By default:

```python
dropna = True
```

Rows where either factor is null are excluded.

For example:

```python
df = pl.DataFrame({
    'group': ['A', None, 'B'],
    'category': ['x', 'x', 'y'],
})
```

```python
crosstab(
    'group',
    'category',
    data=df,
)
```

excludes the row whose `group` is null.

To retain nulls as categories:

```python
crosstab(
    'group',
    'category',
    data=df,
    dropna=False,
)
```

A null index category is preserved even when `fill_value` is supplied.

---

## Null Values

`dropna` applies to the row and column factors, not to the optional `values` column.

Nulls in `values` are handled according to the aggregation.

For example:

```python
aggfunc = 'count'
```

counts only non-null values.

Other aggregations use the corresponding polars behavior.

---

# Filling Missing Cells

Use `fill_value` to replace null result cells.

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='mean',
    fill_value=0,
)
```

Only generated value columns are filled.

The index column is never modified by `fill_value`, so a legitimate null index category remains null.

For frequency tables and normalized tables, unobserved combinations are automatically represented as zero.

---

# Margin Name Conflicts

The margin name must not conflict with an existing category that would occupy the same location.

For example:

```python
df = pl.DataFrame({
    'group': ['A', 'B'],
    'category': ['all', 'x'],
})
```

then:

```python
crosstab(
    'group',
    'category',
    data=df,
    margins=True,
    margins_name='all',
)
```

raises an error rather than silently overwriting the real `'all'` category.

The same protection applies to index categories when a margin row is required.

# Implementation Notes

## Why Aggregation Happens Before Pivoting

Internally, `crosstab` first computes a long-form aggregation:

```text
index   column   value
A       x        ...
A       y        ...
B       x        ...
B       y        ...
```

Only after aggregation, normalization, and margin calculation does it reshape the result into a wide table.

Conceptually:

```text
resolve inputs
      ↓
build long DataFrame
      ↓
filter factor nulls
      ↓
group and aggregate
      ↓
calculate margins from source rows
      ↓
normalize
      ↓
pivot to wide format
      ↓
fill missing result cells
```

This design has several advantages.

First, aggregations such as `n_unique` do not depend on whether `DataFrame.pivot` accepts a corresponding aggregation
string.

Second, margins can be correctly recomputed from the original observations rather than from already-aggregated cells.

Third, normalization can be performed on the appropriate long-form aggregates before the dynamically generated columns
are created.

---

## Eager Execution

`crosstab` returns a `pl.DataFrame` rather than a `LazyFrame`.

A crosstab dynamically creates output columns based on values present in the column factor.

For example:

```text
category = ['yes', 'no', 'unknown']
```

may result in:

```text
yes
no
unknown
```

becoming new columns.

Because the resulting schema depends on values observed in the data, cross-tabulation is naturally an eager operation.

For large lazy workflows, it is often useful to perform filtering and other reductions before collecting:

```python
df = (
    lf
    .filter(...)
    .select('group', 'category')
    .collect()
)

result = crosstab(
    'group',
    'category',
    data=df,
)
```

---

# Examples

## Frequency Table

```python
crosstab(
    'sex',
    'smoker',
    data=df,
)
```

---

## Row Percentages

```python
crosstab(
    'sex',
    'smoker',
    data=df,
    normalize='index',
)
```

---

## Column Percentages

```python
crosstab(
    'sex',
    'smoker',
    data=df,
    normalize='columns',
)
```

---

## Overall Percentages

```python
crosstab(
    'sex',
    'smoker',
    data=df,
    normalize='all',
)
```

---

## Frequency Table With Totals

```python
crosstab(
    'sex',
    'smoker',
    data=df,
    margins=True,
)
```

---

## Row Percentages With Overall Distribution

```python
crosstab(
    'sex',
    'smoker',
    data=df,
    normalize='index',
    margins=True,
)
```

---

## Mean by Two Factors

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='mean',
)
```

---

## Mean With Margins

```python
crosstab(
    'group',
    'category',
    values='score',
    data=df,
    aggfunc='mean',
    margins=True,
)
```

Margins in this example represent means calculated from the original observations, not sums or means of the displayed
cell means.

---

## Unique Counts

```python
crosstab(
    'site',
    'category',
    values='patient_id',
    data=df,
    aggfunc='n_unique',
)
```

---

# Comparison With `pivot`

A simple frequency crosstab can sometimes be expressed directly with `pivot`:

```python
df.pivot(
    index='group',
    on='category',
    aggregate_function='len',
)
```

`crosstab` adds higher-level statistical-table behavior around that basic reshape:

- frequency counts,
- value aggregation,
- `count` and `n_unique`,
- normalization,
- aggregate margins,
- correct non-additive margins,
- normalized margins,
- null-category handling,
- zero-filled frequency combinations, and
- margin-name conflict checking.

Use `pivot` when the primary task is reshaping data.

Use `crosstab` when the desired result is a contingency or summary table.

## Limitations

`crosstab` covers the common two-factor cross-tabulation use case, but it does not reproduce every feature of
`pandas.crosstab`.

- Supports one row factor and one column factor only; pandas can use multiple factors through `MultiIndex`.
- Returns a regular polars `DataFrame` with no hierarchical row or column labels.
- Operates eagerly because generated columns depend on values observed in the data.
- Supports a fixed set of aggregations rather than arbitrary Python callables.
- Does not automatically expand predefined but unobserved categorical levels.
- Does not provide pandas-specific index behavior or exact pandas compatibility.
- Does not perform statistical tests such as chi-squared or Fisher's exact test.
- Does not provide presentation-oriented formatting such as `n (%)`, percentage strings, or styled output.

Use `pandas.crosstab` when hierarchical factors, categorical level expansion, or exact pandas behavior are required. Use
direct polars `group_by`/`pivot` expressions when more specialized aggregation logic is needed.

### Using pandas for multi-factor crosstabs

For more complex tables with multiple row or column factors, convert the polars DataFrame to pandas and use
`pandas.crosstab`.

For example, to cross-tabulate two row variables against a third variable:

```python
import pandas as pd
import polars as pl

df = pl.DataFrame({
    'sex': ['F', 'F', 'M', 'M', 'M'],
    'age_group': ['18-39', '40-64', '18-39', '40-64', '40-64'],
    'smoker': ['no', 'yes', 'no', 'no', 'yes'],
})

pdf = df.to_pandas()

table = pd.crosstab(
    index=[pdf['sex'], pdf['age_group']],
    columns=pdf['smoker'],
)

print(table)
```

This produces a pandas table with a hierarchical (`MultiIndex`) row index:

```text
smoker          no  yes
sex age_group
F   18-39        1    0
    40-64        0    1
M   18-39        1    0
    40-64        1    1
```

This approach is useful when multiple factors on an axis or other pandas-specific crosstab features are needed.
