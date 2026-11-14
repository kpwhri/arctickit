from datetime import date, datetime

import polars as pl


def make_cast_to_date_expr(schema, date_col):
    return to_date_expr(schema, date_col)


def to_date_expr(schema, column: str, out_col: str | None = None, *, format: str | None = None,
                 strict: bool = True) -> pl.Expr:
    """Convert a polars String, Date, Datetime, or Null column to a Date expression.

    Pass ``df.schema`` or ``lf.collect_schema()``.

    * Strings are parsed
    * Datetime values have their time component removed
    * Null column becomes a typed Date null
    """
    dtype = schema[column]
    expr = pl.col(column)

    if dtype == pl.String:
        expr = expr.str.to_date(format=format, strict=strict)
    elif dtype == pl.Date:
        pass
    elif dtype.base_type() == pl.Datetime:
        expr = expr.dt.date()
    elif dtype == pl.Null:
        expr = expr.cast(pl.Date)
    else:
        raise TypeError(f'Unsupported date column dtype: {dtype}')

    return expr.alias(column if out_col is None else out_col)


def to_date_lit_expr(value, out_col: str, *, format: str | None = None, strict: bool = True) -> pl.Expr:
    """Convert a Python string, date, datetime, or None to a polars Date expression."""
    if isinstance(value, str):
        expr = pl.lit(value).str.to_date(format=format, strict=strict)
    elif isinstance(value, datetime):
        expr = pl.lit(value).dt.date()
    elif isinstance(value, date) or value is None:
        expr = pl.lit(value, dtype=pl.Date)
    else:
        raise TypeError(f'Unsupported date literal type: {type(value).__name__}')

    return expr.alias(out_col)
