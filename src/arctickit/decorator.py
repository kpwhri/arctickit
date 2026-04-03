from typing import TypeVar, Callable, ParamSpec
from functools import wraps

import polars as pl

from arctickit.types import PolarsFrame

P = ParamSpec('P')


def requires_lazy(func: Callable[P, pl.LazyFrame]) -> Callable[P, PolarsFrame]:
    """
    Decorator to let a function accept either a DataFrame or LazyFrame,
    operate internally as LazyFrame, and return the same type as input.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> PolarsFrame:
        # detect first positional PolarsFrame (adjust as needed)
        df_arg_index = None
        for i, arg in enumerate(args):
            if isinstance(arg, (pl.DataFrame, pl.LazyFrame)):
                df_arg_index = i
                break

        if df_arg_index is None:
            # fallback: check kwargs
            for key, val in kwargs.items():
                if isinstance(val, (pl.DataFrame, pl.LazyFrame)):
                    df_arg_index = key
                    break

        if df_arg_index is None:
            # no PolarsFrame found; just call normally
            return func(*args, **kwargs)

        # extract input type
        input_df = args[df_arg_index] if isinstance(df_arg_index, int) else kwargs[df_arg_index]
        was_eager = isinstance(input_df, pl.DataFrame)

        if was_eager:
            # convert to LazyFrame
            lazy_df = input_df.lazy()
            # replace argument with lazy_df
            if isinstance(df_arg_index, int):
                args = args[:df_arg_index] + (lazy_df,) + args[df_arg_index + 1:]
            else:
                kwargs[df_arg_index] = lazy_df

        # call original function
        result = func(*args, **kwargs)

        # convert back if needed
        return result.collect() if was_eager else result

    return wrapper


def requires_eager(func: Callable[P, pl.DataFrame]) -> Callable[P, pl.DataFrame]:
    """
    Decorator that ensures the first PolarsFrame argument is eager (DataFrame).
    If input is LazyFrame, it will be collected before calling the function.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> pl.DataFrame:
        # find first positional PolarsFrame argument
        df_arg_index = None
        for i, arg in enumerate(args):
            if isinstance(arg, (pl.DataFrame, pl.LazyFrame)):
                df_arg_index = i
                break
        if df_arg_index is None:
            for key, val in kwargs.items():
                if isinstance(val, (pl.DataFrame, pl.LazyFrame)):
                    df_arg_index = key
                    break

        if df_arg_index is None:
            # no PolarsFrame argument found; call normally
            return func(*args, **kwargs)

        # convert LazyFrame to eager DataFrame if needed
        input_df = args[df_arg_index] if isinstance(df_arg_index, int) else kwargs[df_arg_index]
        was_lazy = isinstance(input_df, pl.LazyFrame)
        if isinstance(input_df, pl.LazyFrame):
            input_df = input_df.collect()

            if isinstance(df_arg_index, int):
                args = args[:df_arg_index] + (input_df,) + args[df_arg_index + 1:]
            else:
                kwargs[df_arg_index] = input_df

        # call original function
        result = func(*args, **kwargs)

        # convert back if needed
        return result.lazy() if was_lazy else result

    return wrapper
