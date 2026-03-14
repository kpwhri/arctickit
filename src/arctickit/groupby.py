"""
Groupby-like elements which seem to be missing in polars.

* crosstab
"""
from __future__ import annotations

from typing import Any, Iterable, Literal, Sequence

import polars as pl

Normalize = Literal[None, True, 'all', 'index', 'columns']
AggFunc = Literal['count', 'sum', 'mean', 'min', 'max', 'median', 'first', 'last', 'n_unique']


def _to_series(data: pl.DataFrame | None, x: str | pl.Series | Sequence[Any], name: str) -> pl.Series:
    """Return a Polars Series from a column name, a Series, or a sequence.

    Parameters
    ----------
    data : pl.DataFrame | None
        Optional DataFrame providing column lookup when `x` is a string.
    x : str | pl.Series | Sequence[Any]
        Column name, Series, or sequence of values.
    name : str
        Fallback series name to use when a name cannot be inferred.
    """
    if isinstance(x, str):
        if data is None:
            raise ValueError('x was a string but no DataFrame was provided to resolve the column')
        if x not in data.columns:
            raise KeyError(f'Column {x!r} not found in DataFrame: {data.columns!r}')
        return data[x]
    if isinstance(x, pl.Series):
        return x.rename(name) if x.name is None else x
    return pl.Series(name, list(x))  # treat as sequence


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
    """Compute a cross-tabulation of two (or more) factors using polars.

    This function mirrors common parts of ``pandas.crosstab`` API while operating on Polars data.

    Parameters
    ----------
    index : str | pl.Series | Sequence
        Row categories. If a string, interpreted as a column of ``data``.
    columns : str | pl.Series | Sequence
        Column categories. If a string, interpreted as a column of ``data``.
    values : str | pl.Series | Sequence | None, default None
        Values to aggregate. If None, a count aggregation is performed.
    data : pl.DataFrame | None, keyword-only
        Optional DataFrame providing columns when inputs are strings.
    aggfunc : {'count','sum','mean','min','max','median','first','last','n_unique'}
        Aggregation to apply to ``values``. Ignored when ``values is None`` and ``aggfunc`` is not 'count'.
    normalize : {None, True, 'all', 'index', 'columns'}
        If specified, compute proportions over the whole table ('all'), by rows ('index'), or by columns ('columns').
        ``True`` is an alias for ``'all'``.
    margins : bool, default False
        Add row/column totals named by ``margins_name``. For normalized output, margins will contain 1.0 on the
        normalized axis, similar to pandas.
    margins_name : str, default 'All'
        Name of the totals row/column when ``margins=True``.
    dropna : bool, default True
        Exclude missing values in inputs. If False, include nulls as a category.
    fill_value : Any | None
        Replace nulls in the result with this value (e.g. 0).

    Returns
    -------
    pl.DataFrame
        A wide Polars DataFrame where the first column is the index labels and the remaining columns are one per
        unique value in ``columns``.
    """
    if normalize is True:
        normalize = 'all'
    if normalize not in (None, 'all', 'index', 'columns'):
        raise ValueError("normalize must be one of None, True, 'all', 'index', or 'columns'")

    idx_s = _to_series(data, index, 'index')
    col_s = _to_series(data, columns, 'columns')

    df = pl.DataFrame({'__idx__': idx_s, '__col__': col_s})

    if values is None:
        # emulate counts by adding a unit column and summing
        df = df.with_columns(pl.lit(1).alias('__val__'))
        agg = 'sum'
    else:
        val_s = _to_series(data, values, 'values')
        df = df.with_columns(val_s.alias('__val__'))
        agg = 'count' if aggfunc == 'count' else aggfunc

    if dropna:
        df = df.filter(pl.col('__idx__').is_not_null() & pl.col('__col__').is_not_null())

    # index rows by __idx__, columns generated from __col__, aggregating __val__
    table = df.pivot(index='__idx__', on='__col__', values='__val__', aggregate_function=agg)

    # ensure predictable ordering of columns: sort by column name (string representation for None)
    col_names = [c for c in table.columns if c != '__idx__']
    # sort column labels while keeping types stable by string key
    sorted_cols = sorted(col_names, key=lambda x: str(x))
    table = table.select(['__idx__', *sorted_cols])  # index in first column

    if isinstance(index, str):  # rename index
        idx_name = index
    else:
        idx_name = getattr(idx_s, 'name', None) or 'index'
    table = table.rename({'__idx__': idx_name})

    if fill_value is not None:
        table = table.fill_null(fill_value)

    if normalize is not None:
        value_cols = [c for c in table.columns if c != idx_name]
        if normalize == 'all':
            # compute total sum as a scalar of all value cells
            total_sum = 0.0
            for c in value_cols:
                total_sum += float(table.select(pl.sum(c)).item() or 0.0)
            if total_sum == 0.0:
                table = table.with_columns([pl.lit(0.0).alias(c) for c in value_cols])
            else:
                table = table.with_columns([pl.col(c) / total_sum for c in value_cols])
        elif normalize == 'index':
            table = table.with_columns(
                pl.sum_horizontal([pl.col(c) for c in value_cols]).alias('__rowsum__')
            ).with_columns([
                (pl.col(c) / pl.col('__rowsum__')).alias(c) for c in value_cols
            ]).drop('__rowsum__')
        elif normalize == 'columns':
            # compute column-wise sums and divide
            col_sums = table.select([pl.sum(c).alias(c) for c in value_cols])
            # avoid division by zero by replacing 0 sums with 1 for division and then fixing results
            safe_divs: list[pl.Expr] = []
            for c in value_cols:
                denom = float(col_sums[c][0]) if col_sums.height > 0 else 0.0
                if denom == 0:
                    safe_divs.append(pl.lit(0.0).alias(c))
                else:
                    safe_divs.append((pl.col(c) / denom).alias(c))
            table = table.with_columns(safe_divs)

    if margins:
        value_cols = [c for c in table.columns if c != idx_name]
        if normalize is None:
            # compute totals in original scale
            row_tot = pl.sum_horizontal(pl.all().exclude(idx_name)).alias(margins_name)
            table = table.with_columns(row_tot)
            col_tot_df = table.select([pl.lit(margins_name).alias(idx_name), *[pl.sum(c).alias(c) for c in value_cols]])
            # sum for the margins column as well
            grand_total = col_tot_df.select(pl.sum_horizontal(pl.all().exclude(idx_name))).item()
            col_tot_df = col_tot_df.with_columns(pl.lit(grand_total).alias(margins_name))
            table = pl.concat([table, col_tot_df], how='vertical_relaxed')
        elif normalize == 'all':
            # all proportions sum to 1 across entire table
            row_tot = pl.lit(1.0).alias(margins_name)
            table = table.with_columns(row_tot)
            bottom = pl.DataFrame({idx_name: [margins_name], **{c: [1.0] for c in value_cols}, margins_name: [1.0]})
            table = pl.concat([table, bottom], how='vertical_relaxed')
        elif normalize == 'index':
            # Row sums are 1.0 across value columns; set margins column to 0.0 for regular rows
            table = table.with_columns(pl.lit(0.0).alias(margins_name))
            # Add a bottom row of ones and 1.0 in bottom-right
            bottom = pl.DataFrame({idx_name: [margins_name], **{c: [1.0] for c in value_cols}, margins_name: [1.0]})
            table = pl.concat([table, bottom], how='vertical_relaxed')
        elif normalize == 'columns':
            # column sums are 1.0, but row sums can be greater
            row_tot = pl.sum_horizontal(pl.all().exclude(idx_name)).alias(margins_name)
            # under column normalization, each column sums to 1, margins column equals row sums
            table = table.with_columns(row_tot)
            bottom = pl.DataFrame(
                {idx_name: [margins_name], **{c: [1.0] for c in value_cols}, margins_name: [float(len(value_cols))]})
            # pandas uses 1.0 in bottom-right for columns normalization; adjust to 1.0
            bottom = bottom.with_columns(pl.lit(1.0).alias(margins_name))
            table = pl.concat([table, bottom], how='vertical_relaxed')

        # margins may have introduced nulls?
        if fill_value is not None:
            table = table.fill_null(fill_value)

    return table
