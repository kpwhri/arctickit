"""Stream sqlalchemy query results into reusable polars/parquet datasets.

Only synchronous sqlalchemy Engine instances are supported.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable, Iterable, Iterator, Sequence
from pathlib import Path

import polars as pl
from sqlalchemy.engine import Engine

FrameTransform = Callable[[pl.DataFrame], pl.DataFrame]


def _scan(output_dir: Path) -> pl.LazyFrame:
    return pl.scan_parquet(str(output_dir / 'part-*.parquet'))


def _write_frames(frames: Iterable[pl.DataFrame], output_dir: str | Path, compression: str) -> pl.LazyFrame:
    """Publish a complete set of parquet files, or remove staged output on error."""
    output_dir = Path(output_dir)
    # ensure parent dir exists
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists():
        raise FileExistsError(f'Output directory already exists: {output_dir}')

    staging_dir = Path(tempfile.mkdtemp(prefix=f'.{output_dir.name}-', dir=output_dir.parent))
    try:
        schema = None
        empty_frame = None
        file_count = 0

        for df in frames:
            if not isinstance(df, pl.DataFrame):
                raise TypeError('Each batch or transform must produce a pl.DataFrame')
            if schema is None:
                schema = df.schema
                empty_frame = df.head(0)
            elif df.schema != schema:
                raise ValueError(
                    'Incompatible output schemas across batches; '
                    'use schema_overrides or cast types in transform'
                )

            if not df.is_empty():
                df.write_parquet(
                    staging_dir / f'part-{file_count:08d}.parquet',
                    compression=compression,
                    statistics=True,
                )
                file_count += 1

        if schema is None:
            raise ValueError('The query returned no batches; output schema is unknown')

        if file_count == 0:
            assert empty_frame is not None
            empty_frame.write_parquet(
                staging_dir / 'part-000000.parquet', compression=compression
            )

        # staging is on the same filesystem as the destination
        if output_dir.exists():
            raise FileExistsError(f'Output directory already exists: {output_dir}')
        os.rename(staging_dir, output_dir)
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return _scan(output_dir)


def _extract(
        *,
        query: str,
        engine: Engine,
        output_dir: str | Path,
        batch_size: int,
        compression: str,
        transform_batches: Callable[[Iterable[pl.DataFrame]], Iterable[pl.DataFrame]],
        schema_overrides: dict | None,
        infer_schema_length: int | None,
        execute_options: dict | None,
) -> pl.LazyFrame:
    if batch_size <= 0:
        raise ValueError('batch_size must be positive')
    if Path(output_dir).exists():
        raise FileExistsError(f'Output directory already exists: {output_dir}')

    with engine.connect().execution_options(yield_per=batch_size) as connection:
        batches = pl.read_database(
            query=query,
            connection=connection,
            iter_batches=True,
            batch_size=batch_size,
            schema_overrides=schema_overrides,
            infer_schema_length=infer_schema_length,
            execute_options=execute_options,
        )
        try:
            return _write_frames(transform_batches(batches), output_dir, compression=compression)
        finally:
            close = getattr(batches, 'close', None)
            if callable(close):
                close()


def _transformed_batches(batches: Iterable[pl.DataFrame], transform: FrameTransform | None) -> Iterator[pl.DataFrame]:
    for df in batches:
        yield transform(df) if transform is not None else df


def read_database_to_parquet(
        query: str,
        engine: Engine,
        output_dir: str | Path,
        *,
        transform: FrameTransform | None = None,
        batch_size: int = 100_000,
        compression: str = 'zstd',
        schema_overrides: dict | None = None,
        infer_schema_length: int | None = 100,
        execute_options: dict | None = None,
) -> pl.LazyFrame:
    """Run a SQL query once and write (optionally transformed) batches to Parquet.

    A batch-local transform can filter, select, or add columns. It does not
    perform global aggregations/deduplication across database batches.
    The output directory must not already exist.
    """
    return _extract(
        query=query,
        engine=engine,
        output_dir=output_dir,
        batch_size=batch_size,
        compression=compression,
        transform_batches=lambda batches: _transformed_batches(batches, transform),
        schema_overrides=schema_overrides,
        infer_schema_length=infer_schema_length,
        execute_options=execute_options,
    )


def _same_group(left: pl.DataFrame, right: pl.DataFrame, keys: tuple[str, ...]) -> bool:
    """Compare first rows' group keys, including null keys."""
    key_pair = pl.concat(
        [left.head(1).select(keys), right.head(1).select(keys)],
        rechunk=False,
    )
    equal_to_previous = pl.all_horizontal(
        *(pl.col(k).eq_missing(pl.col(k).shift(1)) for k in keys)
    )
    return bool(key_pair.select(equal_to_previous).to_series()[-1])


def _grouped_batches(
        batches: Iterable[pl.DataFrame],
        keys: tuple[str, ...],
        aggregations: tuple[pl.Expr, ...],
        transform: FrameTransform | None,
) -> Iterator[pl.DataFrame]:
    """Holds raw fragments of *one* unfinished group, without repeatedly
    re-concatenating a very large group on every database fetch.
    """
    pending: list[pl.DataFrame] = []
    empty_template: pl.DataFrame | None = None

    def aggregate(parts: Sequence[pl.DataFrame]) -> pl.DataFrame:
        return (
            pl.concat(parts, rechunk=False)
            .group_by(*keys, maintain_order=True)
            .agg(*aggregations)
        )

    for raw in batches:
        df = transform(raw) if transform is not None else raw
        if not isinstance(df, pl.DataFrame):
            raise TypeError('transform must return a pl.DataFrame')
        missing_key_columns = set(keys) - set(df.columns)
        if missing_key_columns:
            raise ValueError(f'Missing grouping columns: {sorted(missing_key_columns)}')
        if empty_template is None:
            empty_template = df.head(0)
        if df.is_empty():
            continue

        # find group transitions *within this batch*, ignoring index 0
        # (ne_missing treats consecutive null group keys equal)
        change = pl.any_horizontal(
            *(pl.col(k).ne_missing(pl.col(k).shift(1)) for k in keys)
        )
        starts = df.select(pl.arg_where(change)).to_series()
        cuts = starts.filter(starts > 0)

        first_end = int(cuts[0]) if len(cuts) else df.height
        first_group = df.slice(0, first_end)
        finished: list[pl.DataFrame] = []

        if pending:
            if _same_group(pending[0], first_group, keys):
                pending.append(first_group)
            else:
                finished.append(aggregate(pending))
                pending = [first_group]
        else:
            pending = [first_group]

        if len(cuts):
            # first group is complete since new groups appears in this batch
            finished.append(aggregate(pending))
            pending = []

            last_start = int(cuts[-1])
            if first_end < last_start:
                # all intervening groups are contained in this batch
                complete_middle = df.slice(first_end, last_start - first_end)
                finished.append(
                    complete_middle.group_by(*keys, maintain_order=True).agg(
                        *aggregations
                    )
                )

            # final group might continue in the next batch.
            pending = [df.slice(last_start)]

        if finished:
            yield pl.concat(finished, rechunk=False)

    if pending:
        yield aggregate(pending)
    elif empty_template is not None:
        # all rows were filtered out (or the query returned an empty batch)
        yield empty_template.group_by(*keys, maintain_order=True).agg(*aggregations)


def read_grouped_database_to_parquet(
        query: str,
        engine: Engine,
        output_dir: str | Path,
        *,
        group_by: str | Sequence[str],
        aggregations: Sequence[pl.Expr],
        transform: FrameTransform | None = None,
        batch_size: int = 100_000,
        compression: str = 'zstd',
        schema_overrides: dict | None = None,
        infer_schema_length: int | None = 100,
        execute_options: dict | None = None,
) -> pl.LazyFrame:
    """Stream SQL, aggregate complete contiguous groups, and write parquet directory.

    NOTES:
    * Query results must be ordered so all rows for each complete group key are contiguous
        (normally ORDER BY group columns, sequence column)
    * If supplied, transform must preserve that contiguity and row order
    * Arbitrary Polars aggregate expressions are supported for each full group
    * Memory may grow to hold the largest unfinished group
    """
    keys = (group_by,) if isinstance(group_by, str) else tuple(group_by)
    aggs = tuple(aggregations)
    if not keys or any(not isinstance(key, str) for key in keys):
        raise ValueError('group_by must contain at least one column name')
    if not aggs or any(not isinstance(agg, pl.Expr) for agg in aggs):
        raise ValueError('aggregations must contain at least one Polars expression')

    return _extract(
        query=query,
        engine=engine,
        output_dir=output_dir,
        batch_size=batch_size,
        compression=compression,
        transform_batches=lambda batches: _grouped_batches(batches, keys, aggs, transform),
        schema_overrides=schema_overrides,
        infer_schema_length=infer_schema_length,
        execute_options=execute_options,
    )


def get_or_create_parquet(
        query: str,
        engine: Engine,
        output_dir: str | Path,
        *,
        group_by: str | Sequence[str] | None = None,
        aggregations: Sequence[pl.Expr] | None = None,
        **kwargs,
) -> pl.LazyFrame:
    """Return an existing dataset, or extract one (grouped or ungrouped).

    output_dir is the cache key: changing SQL/options/transform does not
    invalidate the directory. Use a different path to refresh or version it.
    """
    if (group_by is None) != (aggregations is None):
        raise ValueError('Pass both group_by and aggregations, or neither')

    output_dir = Path(output_dir)
    if output_dir.exists():
        if not output_dir.is_dir() or not any(output_dir.glob('part-*.parquet')):
            raise ValueError(f'Existing path is not a Parquet dataset: {output_dir}')
        return _scan(output_dir)

    if group_by is not None:
        return read_grouped_database_to_parquet(
            query=query,
            engine=engine,
            output_dir=output_dir,
            group_by=group_by,
            aggregations=aggregations,
            **kwargs,
        )

    return read_database_to_parquet(query=query, engine=engine, output_dir=output_dir, **kwargs)
