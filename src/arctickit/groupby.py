"""
Groupby-like elements which seem to be missing in polars.

* crosstab
"""
from __future__ import annotations

from typing import Any, Literal, Sequence, TypeAlias

import polars as pl

AggFunc: TypeAlias = Literal[
    'count', 'sum', 'mean', 'min', 'max',
    'median', 'first', 'last', 'n_unique',
]
Normalize: TypeAlias = Literal['all', 'index', 'columns'] | bool | None


def _to_series(
        data: pl.DataFrame | None,
        value: str | pl.Series | Sequence[Any],
        name: str,
) -> pl.Series:
    """Resolve a column name, Series, or sequence to a Series."""
    if isinstance(value, str):
        if data is None:
            raise ValueError(f'data is required when {name} is a column name')
        return data.get_column(value)

    if isinstance(value, pl.Series):
        return value

    return pl.Series(name, value)


def _agg_expr(aggfunc: AggFunc, *, frequency: bool) -> pl.Expr:
    """Return the Polars aggregation expression."""
    if frequency:
        return pl.len()

    value = pl.col('__val__')
    aggfuncs = {
        'count': value.count(),
        'sum': value.sum(),
        'mean': value.mean(),
        'min': value.min(),
        'max': value.max(),
        'median': value.median(),
        'first': value.first(),
        'last': value.last(),
        'n_unique': value.n_unique(),
    }

    try:
        return aggfuncs[aggfunc]
    except KeyError:
        raise ValueError(f'unsupported aggfunc: {aggfunc!r}') from None


def _aggregate(df: pl.DataFrame, by: list[str] | None, expr: pl.Expr) -> pl.DataFrame:
    """Aggregate with optional grouping."""
    if by:
        return (
            df.group_by(by, maintain_order=True)
            .agg(expr.alias('__value__'))
        )

    return df.select(expr.alias('__value__'))


def _normalize(
        df: pl.DataFrame,
        over: str | None = None,
) -> pl.DataFrame:
    """Normalize the __value__ column globally or within groups."""
    value = pl.col('__value__').cast(pl.Float64).fill_null(0.0)
    denom = value.sum() if over is None else value.sum().over(over)

    return df.with_columns(
        pl.when(denom.is_null() | (denom == 0))
        .then(0.0)
        .otherwise(value / denom)
        .alias('__value__')
    )


def _contains_label(series: pl.Series, label: str) -> bool:
    """Return whether a factor contains a label after string conversion."""
    return any(
        value is not None and str(value) == label
        for value in series
    )


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
    """Compute a cross-tabulation of two factors using Polars.

    Parameters
    ----------
    index
        Row factor. A string refers to a column in `data`; otherwise
        provide a Series or sequence of factor values.
    columns
        Column factor. A string refers to a column in `data`; otherwise
        provide a Series or sequence of factor values.
    values
        Values to aggregate. If omitted, the table contains frequencies.
    data
        DataFrame used to resolve string column names.
    aggfunc
        Aggregation applied when `values` is provided. Supported values
        are 'count', 'sum', 'mean', 'min', 'max', 'median', 'first',
        'last', and 'n_unique'. When `values` is omitted, frequency
        counts are used regardless of this argument.
    normalize
        Normalize the result over the entire table ('all'), within rows
        ('index'), or within columns ('columns'). True is equivalent to
        'all'; False is equivalent to None.
    margins
        Add aggregate margins. Margins are calculated directly from the
        source observations rather than from already-aggregated cells.
    margins_name
        Label used for margin rows and columns.
    dropna
        If True, exclude rows where either factor is null. Nulls in
        `values` remain subject to the selected aggregation.
    fill_value
        Replace null result cells with this value. The index column is
        never filled.

    Returns
    -------
    pl.DataFrame
        Wide cross-tabulation with the row factor in the first column.

    Notes
    -----
    A sequence supplied to `index` or `columns` represents factor values,
    not multiple factor columns.

    `count` counts non-null values. `n_unique` follows Polars semantics,
    where null is considered a unique value.

    With normalized margins, 'index' adds only a margin row and 'columns'
    adds only a margin column, matching pandas-style crosstab behavior.
    """
    if normalize is True:
        normalize = 'all'
    elif normalize is False:
        normalize = None

    if normalize not in (None, 'all', 'index', 'columns'):
        raise ValueError(
            "normalize must be one of None, True, False, 'all', "
            "'index', or 'columns'"
        )

    idx_s = _to_series(data, index, 'index')
    col_s = _to_series(data, columns, 'columns')
    val_s = None if values is None else _to_series(data, values, 'values')

    series = [idx_s, col_s] + ([] if val_s is None else [val_s])
    if len({len(s) for s in series}) != 1:
        raise ValueError('index, columns, and values must have equal lengths')

    idx_name = (
        index if isinstance(index, str)
        else idx_s.name or 'index'
    )

    frequency = values is None
    expr = _agg_expr(aggfunc, frequency=frequency)

    cols = {
        '__idx__': idx_s,
        '__col__': col_s,
    }
    if val_s is not None:
        cols['__val__'] = val_s

    df = pl.DataFrame(cols)

    if dropna:
        df = df.filter(
            pl.col('__idx__').is_not_null()
            & pl.col('__col__').is_not_null()
        )

    add_margin_col = (
            margins and normalize in (None, 'all', 'columns')
    )
    add_margin_row = (
            margins and normalize in (None, 'all', 'index')
    )

    if add_margin_col:
        if idx_name == margins_name:
            raise ValueError(
                f'margins_name {margins_name!r} conflicts with index name'
            )
        if _contains_label(col_s, margins_name):
            raise ValueError(
                f'margins_name {margins_name!r} conflicts with a column category'
            )

    if add_margin_row and _contains_label(idx_s, margins_name):
        raise ValueError(
            f'margins_name {margins_name!r} conflicts with an index category'
        )

    cells = _aggregate(df, ['__idx__', '__col__'], expr)

    if margins:
        row_margin = _aggregate(df, ['__idx__'], expr)
        col_margin = _aggregate(df, ['__col__'], expr)
        grand_margin = _aggregate(df, None, expr)

    if normalize == 'all':
        cells = _normalize(cells)

        if margins:
            row_margin = _normalize(row_margin)
            col_margin = _normalize(col_margin)
            grand_margin = grand_margin.with_columns(
                pl.lit(1.0).alias('__value__')
            )

    elif normalize == 'index':
        cells = _normalize(cells, '__idx__')

        if margins:
            col_margin = _normalize(col_margin)

    elif normalize == 'columns':
        cells = _normalize(cells, '__col__')

        if margins:
            row_margin = _normalize(row_margin)

    parts = [cells]

    if add_margin_col:
        parts.append(
            row_margin
            .with_columns(pl.lit(margins_name).alias('__col__'))
            .select('__idx__', '__col__', '__value__')
        )

    if add_margin_row:
        parts.append(
            col_margin
            .with_columns(pl.lit(margins_name).alias('__idx__'))
            .select('__idx__', '__col__', '__value__')
        )

    if margins and normalize in (None, 'all'):
        parts.append(
            grand_margin
            .with_columns(
                pl.lit(margins_name).alias('__idx__'),
                pl.lit(margins_name).alias('__col__'),
            )
            .select('__idx__', '__col__', '__value__')
        )

    long_df = (
        cells
        if len(parts) == 1
        else pl.concat(parts, how='vertical_relaxed')
    )

    table = long_df.pivot(
        on='__col__',
        index='__idx__',
        values='__value__',
        aggregate_function='first',
        maintain_order=True,
        sort_columns=True,
    )

    value_cols = [c for c in table.columns if c != '__idx__']

    # Keep the margin column last.
    if add_margin_col and margins_name in value_cols:
        value_cols = [
                         c for c in value_cols if c != margins_name
                     ] + [margins_name]
        table = table.select('__idx__', *value_cols)

    if idx_name in value_cols:
        raise ValueError(
            f'index name {idx_name!r} conflicts with a generated column'
        )

    # Frequency tables and normalized tables represent absent cells as zero.
    if value_cols and (frequency or normalize is not None):
        table = table.with_columns(
            pl.col(value_cols).fill_null(0)
        )

    if value_cols and fill_value is not None:
        table = table.with_columns(
            pl.col(value_cols).fill_null(fill_value)
        )

    return table.rename({'__idx__': idx_name})
