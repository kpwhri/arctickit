import polars as pl


def all_columns_non_null_expr(*columns: str) -> pl.Expr:
    """Return a Polars expression requiring every specified column to be non-null.

    The returned expression evaluates row-wise to `True` only when all named
    columns contain non-null values for that row. It is intended for use in
    Polars filtering or conditional expressions.

    Parameters:
        *columns: Names of columns that must all be non-null.

    Returns:
        A Boolean Polars expression.
    """
    return pl.all_horizontal(
        *(pl.col(column).is_not_null() for column in columns)
    )


def any_flag_column_true_expr(*columns: str) -> pl.Expr:
    """Return a Polars expression indicating whether any specified flag column is true.

    Each named column is cast to Boolean, then combined row-wise with OR logic.
    The returned expression evaluates to `True` when at least one of the columns
    is truthy for that row.

    Parameters:
        *columns: Names of flag-like columns to evaluate.

    Returns:
        A Boolean Polars expression.
    """
    return pl.any_horizontal(
        *(pl.col(column).cast(pl.Boolean) for column in columns)
    )


def classification_indicator_exprs(value_column: str, values: tuple) -> list[pl.Expr]:
    """Return expressions that create one aggregated indicator column per value.

    For each value in `values`, this creates a Polars expression that checks
    whether `value_column` contains that value anywhere in the current aggregation
    group. The result is cast to `Int8`, producing `1` when the value is present
    and `0` when it is absent. Each output column is named after the corresponding
    value.

    Parameters:
        value_column: Name of the column containing classification values.
        values: Classification values to convert into indicator columns.

    Returns:
        A list of Polars expressions, one per classification value.
    """
    return [
        pl.col(value_column).eq(value).any().cast(pl.Int8).alias(value)
        for value in values
    ]
