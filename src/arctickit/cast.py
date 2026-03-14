import polars as pl


def make_cast_to_date_expr(schema, date_col):
    match schema.get(date_col):
        case pl.Datetime:
            return pl.col(date_col).dt.date().alias(date_col)
        case pl.Utf8:
            return pl.col(date_col).str.strptime(pl.Date, strict=False).alias(date_col)
        case pl.Date:
            return None
        case _:
            raise ValueError(f'Expected {date_col} to be pl.Date column, got {schema.get(date_col)}')
