"""Integration and edge-case tests for arctickit.db2parquet."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

import arctickit.db2parquet as db2parquet
from arctickit.db2parquet import (
    get_or_create_parquet,
    read_database_to_parquet,
    read_grouped_database_to_parquet,
)


@pytest.fixture()
def engine() -> Iterable[Engine]:
    """Create a deterministic SQLite database with differently sized groups."""
    db_engine = create_engine('sqlite+pysqlite:///:memory:')
    with db_engine.begin() as connection:
        connection.execute(
            text(
                'CREATE TABLE parts ('
                'doc_id TEXT, seq INTEGER, piece TEXT, keep INTEGER)'
            )
        )
        connection.execute(
            text(
                'INSERT INTO parts (doc_id, seq, piece, keep) '
                'VALUES (:doc_id, :seq, :piece, :keep)'
            ),
            [
                {'doc_id': 'a', 'seq': 0, 'piece': 'hello', 'keep': 1},
                {'doc_id': 'a', 'seq': 1, 'piece': ' ', 'keep': 1},
                {'doc_id': 'a', 'seq': 2, 'piece': 'world', 'keep': 1},
                {'doc_id': 'b', 'seq': 0, 'piece': 'Pol', 'keep': 1},
                {'doc_id': 'b', 'seq': 1, 'piece': 'ars', 'keep': 1},
                {'doc_id': 'c', 'seq': 0, 'piece': 'skip', 'keep': 0},
            ],
        )
    yield db_engine
    db_engine.dispose()


@pytest.fixture()
def composite_engine() -> Iterable[Engine]:
    """Exercise null keys, composite keys, and varying group sizes."""
    db_engine = create_engine('sqlite+pysqlite:///:memory:')
    with db_engine.begin() as connection:
        connection.execute(
            text(
                'CREATE TABLE segments ('
                'region TEXT, doc_id TEXT, seq INTEGER, piece TEXT, amount INTEGER)'
            )
        )
        connection.execute(
            text(
                'INSERT INTO segments (region, doc_id, seq, piece, amount) '
                'VALUES (:region, :doc_id, :seq, :piece, :amount)'
            ),
            [
                {'region': None, 'doc_id': 'a', 'seq': 0, 'piece': 'na', 'amount': 1},
                {'region': None, 'doc_id': 'a', 'seq': 1, 'piece': 'nb', 'amount': 3},
                {'region': None, 'doc_id': 'b', 'seq': 0, 'piece': 'nc', 'amount': 4},
                {'region': 'west', 'doc_id': None, 'seq': 0, 'piece': 'wa', 'amount': 5},
                {'region': 'west', 'doc_id': None, 'seq': 1, 'piece': 'wb', 'amount': 6},
                {'region': 'west', 'doc_id': 'a', 'seq': 0, 'piece': 'wc', 'amount': 7},
                {'region': 'west', 'doc_id': 'a', 'seq': 1, 'piece': 'wd', 'amount': 8},
                {'region': 'west', 'doc_id': 'a', 'seq': 2, 'piece': 'we', 'amount': 9},
            ],
        )
    yield db_engine
    db_engine.dispose()


PARTS_SQL = 'SELECT doc_id, seq, piece, keep FROM parts ORDER BY doc_id, seq'
GROUP_AGGS = (
    pl.col('piece').str.join('').alias('full_text'),
    pl.len().alias('pieces'),
)
EXPECTED_GROUPS = [
    {'doc_id': 'a', 'full_text': 'hello world', 'pieces': 3},
    {'doc_id': 'b', 'full_text': 'Polars', 'pieces': 2},
    {'doc_id': 'c', 'full_text': 'skip', 'pieces': 1},
]


def assert_no_staging_files(parent: Path, destination_name: str) -> None:
    assert not list(parent.glob(f'.{destination_name}-*'))


def fake_batches(monkeypatch, frames: list[pl.DataFrame]) -> None:
    """Replace only database reads; retain real Polars transformations and writes."""
    monkeypatch.setattr(db2parquet.pl, 'read_database', lambda **_kwargs: iter(frames))


@pytest.mark.parametrize('batch_size', [1, 2, 3, 4, 10])
def test_plain_export_matches_sql_rows(tmp_path, engine, batch_size):
    destination = tmp_path / 'plain'
    result = read_database_to_parquet(
        PARTS_SQL, engine, destination, batch_size=batch_size
    )

    assert isinstance(result, pl.LazyFrame)
    assert result.collect().to_dicts() == [
        {'doc_id': 'a', 'seq': 0, 'piece': 'hello', 'keep': 1},
        {'doc_id': 'a', 'seq': 1, 'piece': ' ', 'keep': 1},
        {'doc_id': 'a', 'seq': 2, 'piece': 'world', 'keep': 1},
        {'doc_id': 'b', 'seq': 0, 'piece': 'Pol', 'keep': 1},
        {'doc_id': 'b', 'seq': 1, 'piece': 'ars', 'keep': 1},
        {'doc_id': 'c', 'seq': 0, 'piece': 'skip', 'keep': 0},
    ]
    assert len(list(destination.glob('part-*.parquet'))) >= 1
    assert_no_staging_files(tmp_path, 'plain')


def test_plain_export_transform_filters_projects_and_derives(tmp_path, engine):
    result = read_database_to_parquet(
        PARTS_SQL,
        engine,
        tmp_path / 'transformed',
        batch_size=2,
        transform=lambda df: (
            df.filter(pl.col('keep') == 1)
            .select('doc_id', 'seq', 'piece')
            .with_columns(pl.col('piece').str.to_uppercase().alias('upper_piece'))
        ),
        compression='uncompressed',
    ).collect()

    assert result.columns == ['doc_id', 'seq', 'piece', 'upper_piece']
    assert result.height == 5
    assert result.get_column('upper_piece').to_list() == [
        'HELLO', ' ', 'WORLD', 'POL', 'ARS'
    ]


def test_sql_parameters_and_projection(tmp_path, engine):
    result = read_database_to_parquet(
        'SELECT doc_id, seq FROM parts WHERE seq >= :minimum ORDER BY doc_id, seq',
        engine,
        tmp_path / 'parameters',
        batch_size=1,
        execute_options={'parameters': {'minimum': 1}},
        schema_overrides={'seq': pl.Int64},
        infer_schema_length=10,
    ).collect()

    assert result.to_dicts() == [
        {'doc_id': 'a', 'seq': 1},
        {'doc_id': 'a', 'seq': 2},
        {'doc_id': 'b', 'seq': 1},
    ]
    assert result.schema['seq'] == pl.Int64


def test_streaming_connection_options_and_cleanup(tmp_path, engine, monkeypatch):
    original = pl.read_database
    observed = {}

    def capture(**kwargs):
        observed['options'] = dict(kwargs['connection'].get_execution_options())
        observed['connection'] = kwargs['connection']
        observed['arguments'] = kwargs
        return original(**kwargs)

    monkeypatch.setattr(db2parquet.pl, 'read_database', capture)
    read_database_to_parquet(PARTS_SQL, engine, tmp_path / 'streaming', batch_size=2)

    assert observed['options']['yield_per'] == 2
    # assert observed['options']['stream_results'] is True
    assert observed['arguments']['iter_batches'] is True
    assert observed['arguments']['batch_size'] == 2
    assert observed['connection'].closed
    with engine.connect() as connection:
        assert connection.execute(text('SELECT COUNT(*) FROM parts')).scalar_one() == 6


def test_filter_everything_out_preserves_schema(tmp_path, engine):
    destination = tmp_path / 'empty_after_filter'
    result = read_database_to_parquet(
        PARTS_SQL,
        engine,
        destination,
        batch_size=2,
        transform=lambda df: df.filter(pl.col('keep') == 999),
    )

    assert result.collect().height == 0
    assert result.collect_schema().names() == ['doc_id', 'seq', 'piece', 'keep']
    assert len(list(destination.glob('part-*.parquet'))) == 1


def test_empty_batch_preserves_schema(tmp_path, engine, monkeypatch):
    fake_batches(
        monkeypatch,
        [pl.DataFrame(schema={'doc_id': pl.String, 'seq': pl.Int64})],
    )
    result = read_database_to_parquet('unused', engine, tmp_path / 'empty_batch')

    assert result.collect_schema().names() == ['doc_id', 'seq']
    assert result.collect().height == 0


def test_no_batches_is_an_error_and_cleans_staging(tmp_path, engine, monkeypatch):
    fake_batches(monkeypatch, [])
    destination = tmp_path / 'no_batches'

    with pytest.raises(ValueError, match='no batches'):
        read_database_to_parquet('unused', engine, destination)

    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'no_batches')


@pytest.mark.parametrize('batch_size', [1, 2, 3, 4, 6, 10])
def test_grouped_export_preserves_complete_groups(tmp_path, engine, batch_size):
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        tmp_path / f'grouped_{batch_size}',
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=batch_size,
    )

    assert result.collect().to_dicts() == EXPECTED_GROUPS


def test_grouped_export_joins_one_group_across_many_batches(tmp_path, engine):
    text_parts = ['one', '-', 'two', '-', 'three', '-', 'four', '-', 'five']
    with engine.begin() as connection:
        connection.execute(text('DELETE FROM parts'))
        connection.execute(
            text('INSERT INTO parts VALUES (:doc_id, :seq, :piece, :keep)'),
            [
                {'doc_id': 'one', 'seq': i, 'piece': piece, 'keep': 1}
                for i, piece in enumerate(text_parts)
            ],
        )

    destination = tmp_path / 'long_group'
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        destination,
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=1,
    ).collect()

    assert result.to_dicts() == [{'doc_id': 'one', 'full_text': 'one-two-three-four-five', 'pieces': 9}]
    assert len(list(destination.glob('part-*.parquet'))) == 1


def test_grouped_export_middle_groups_and_exact_boundaries(tmp_path, engine):
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        tmp_path / 'exact_boundary',
        group_by=['doc_id'],
        aggregations=[pl.len().alias('count')],
        batch_size=3,
    ).collect()

    assert result.to_dicts() == [
        {'doc_id': 'a', 'count': 3},
        {'doc_id': 'b', 'count': 2},
        {'doc_id': 'c', 'count': 1},
    ]


@pytest.mark.parametrize('batch_size', [1, 2, 3, 5, 50])
def test_grouped_export_composite_nullable_keys(tmp_path, composite_engine, batch_size):
    query = (
        'SELECT region, doc_id, seq, piece, amount FROM segments '
        'ORDER BY region, doc_id, seq'
    )
    result = read_grouped_database_to_parquet(
        query,
        composite_engine,
        tmp_path / f'composite_{batch_size}',
        group_by=['region', 'doc_id'],
        aggregations=[
            pl.col('piece').str.join('').alias('full_text'),
            pl.col('amount').sum().alias('total'),
            pl.len().alias('pieces'),
        ],
        schema_overrides={'region': pl.String, 'doc_id': pl.String},
        batch_size=batch_size,
    )

    assert result.collect().to_dicts() == [
        {'region': None, 'doc_id': 'a', 'full_text': 'nanb', 'total': 4, 'pieces': 2},
        {'region': None, 'doc_id': 'b', 'full_text': 'nc', 'total': 4, 'pieces': 1},
        {'region': 'west', 'doc_id': None, 'full_text': 'wawb', 'total': 11, 'pieces': 2},
        {'region': 'west', 'doc_id': 'a', 'full_text': 'wcwdwe', 'total': 24, 'pieces': 3},
    ]


def test_grouped_transform_can_filter_out_entire_group(tmp_path, engine):
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        tmp_path / 'keep_only',
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=1,
        transform=lambda df: df.filter(pl.col('keep') == 1),
    ).collect()

    assert result.to_dicts() == EXPECTED_GROUPS[:2]


def test_grouped_transform_can_filter_all_rows(tmp_path, engine):
    destination = tmp_path / 'all_removed'
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        destination,
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=1,
        transform=lambda df: df.filter(pl.col('keep') == 999),
    )

    assert result.collect().is_empty()
    assert result.collect_schema().names() == ['doc_id', 'full_text', 'pieces']
    assert len(list(destination.glob('part-*.parquet'))) == 1


def test_grouped_aggregate_list_and_min_max(tmp_path, engine):
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        tmp_path / 'lists',
        group_by='doc_id',
        aggregations=[
            pl.col('piece').alias('pieces_list'),
            pl.col('seq').min().alias('first_seq'),
            pl.col('seq').max().alias('last_seq'),
        ],
        batch_size=2,
    ).collect()

    assert result.to_dicts() == [
        {'doc_id': 'a', 'pieces_list': ['hello', ' ', 'world'], 'first_seq': 0, 'last_seq': 2},
        {'doc_id': 'b', 'pieces_list': ['Pol', 'ars'], 'first_seq': 0, 'last_seq': 1},
        {'doc_id': 'c', 'pieces_list': ['skip'], 'first_seq': 0, 'last_seq': 0},
    ]


def test_grouped_empty_batch_preserves_aggregated_schema(tmp_path, engine, monkeypatch):
    fake_batches(monkeypatch, [pl.DataFrame(schema={'doc_id': pl.String, 'piece': pl.String})])
    result = read_grouped_database_to_parquet(
        'unused',
        engine,
        tmp_path / 'empty_grouped',
        group_by='doc_id',
        aggregations=[pl.col('piece').str.join('').alias('joined')],
    )

    assert result.collect().is_empty()
    assert result.collect_schema().names() == ['doc_id', 'joined']


def test_plain_cache_hit_does_not_execute_sql(tmp_path, engine):
    destination = tmp_path / 'cache'
    first = get_or_create_parquet(PARTS_SQL, engine, destination, batch_size=2)
    assert first.collect().height == 6

    queries = []

    def before_query(_connection, _cursor, statement, _parameters, _context, _many):
        queries.append(statement)

    event.listen(engine, 'before_cursor_execute', before_query)
    try:
        cached = get_or_create_parquet('SELECT * FROM missing_table', engine, destination, batch_size=1)
        assert cached.collect().to_dicts() == first.collect().to_dicts()
    finally:
        event.remove(engine, 'before_cursor_execute', before_query)

    assert queries == []


def test_grouped_cache_hit_does_not_reaggregate(tmp_path, engine):
    destination = tmp_path / 'cache_grouped'
    first = get_or_create_parquet(
        PARTS_SQL,
        engine,
        destination,
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=2,
    )
    assert first.collect().to_dicts() == EXPECTED_GROUPS

    cached = get_or_create_parquet(
        'SELECT * FROM missing_table',
        engine,
        destination,
        group_by='doc_id',
        aggregations=GROUP_AGGS,
    )
    assert cached.collect().to_dicts() == EXPECTED_GROUPS


@pytest.mark.parametrize('invalid_path', ['empty_dir', 'ordinary_file', 'wrong_files'])
def test_cache_rejects_invalid_existing_path(tmp_path, engine, invalid_path):
    destination = tmp_path / invalid_path
    if invalid_path == 'ordinary_file':
        destination.write_text('not a dataset')
    else:
        destination.mkdir()
        if invalid_path == 'wrong_files':
            (destination / 'extra.txt').write_text('not parquet')

    with pytest.raises(ValueError, match='not a Parquet dataset'):
        get_or_create_parquet(PARTS_SQL, engine, destination)


def test_cache_validates_pairing_even_for_existing_dataset(tmp_path, engine):
    destination = tmp_path / 'cache'
    get_or_create_parquet(PARTS_SQL, engine, destination)

    with pytest.raises(ValueError, match='both group_by and aggregations'):
        get_or_create_parquet(PARTS_SQL, engine, destination, group_by='doc_id')
    with pytest.raises(ValueError, match='both group_by and aggregations'):
        get_or_create_parquet(PARTS_SQL, engine, destination, aggregations=GROUP_AGGS)


@pytest.mark.parametrize('batch_size', [0, -1])
@pytest.mark.parametrize('grouped', [False, True])
def test_invalid_batch_size_fails_before_creating_output(tmp_path, engine, batch_size, grouped):
    destination = tmp_path / 'invalid'
    with pytest.raises(ValueError, match='batch_size must be positive'):
        if grouped:
            read_grouped_database_to_parquet(
                PARTS_SQL, engine, destination,
                batch_size=batch_size, group_by='doc_id', aggregations=GROUP_AGGS,
            )
        else:
            read_database_to_parquet(
                PARTS_SQL, engine, destination, batch_size=batch_size
            )
    assert not destination.exists()


@pytest.mark.parametrize(('group_by', 'aggregations'), [
    ([], GROUP_AGGS),
    (['doc_id', 123], GROUP_AGGS),
    ('doc_id', []),
    ('doc_id', ['piece']),
])
def test_invalid_grouping_arguments(tmp_path, engine, group_by, aggregations):
    destination = tmp_path / 'invalid_grouping'
    with pytest.raises(ValueError):
        read_grouped_database_to_parquet(PARTS_SQL, engine, destination, group_by=group_by, aggregations=aggregations)
    assert not destination.exists()


@pytest.mark.parametrize('grouped', [False, True])
def test_existing_destination_is_not_overwritten(tmp_path, engine, grouped):
    destination = tmp_path / 'existing'
    destination.mkdir()
    marker = destination / 'keep.txt'
    marker.write_text('untouched')

    with pytest.raises(FileExistsError):
        if grouped:
            read_grouped_database_to_parquet(PARTS_SQL, engine, destination, group_by='doc_id', aggregations=GROUP_AGGS)
        else:
            read_database_to_parquet(PARTS_SQL, engine, destination)

    assert marker.read_text() == 'untouched'


@pytest.mark.parametrize('grouped', [False, True])
def test_invalid_sql_leaves_no_partial_output(tmp_path, engine, grouped):
    destination = tmp_path / 'invalid_sql'
    with pytest.raises(Exception):
        if grouped:
            read_grouped_database_to_parquet(
                'SELECT * FROM missing_table', engine, destination,
                group_by='doc_id', aggregations=GROUP_AGGS,
            )
        else:
            read_database_to_parquet('SELECT * FROM missing_table', engine, destination)

    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'invalid_sql')


@pytest.mark.parametrize('grouped', [False, True])
def test_non_dataframe_transform_is_rejected_and_cleaned_up(tmp_path, engine, grouped):
    destination = tmp_path / 'bad_transform'
    options = {'group_by': 'doc_id', 'aggregations': GROUP_AGGS} if grouped else {}

    with pytest.raises(TypeError, match='pl.DataFrame'):
        function = read_grouped_database_to_parquet if grouped else read_database_to_parquet
        function(PARTS_SQL, engine, destination, transform=lambda _df: None, batch_size=1, **options)
    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'bad_transform')


def test_missing_group_column_fails_even_when_batch_empty(tmp_path, engine):
    destination = tmp_path / 'missing_keys'
    with pytest.raises(ValueError, match='Missing grouping columns'):
        read_grouped_database_to_parquet(
            PARTS_SQL,
            engine,
            destination,
            group_by='doc_id',
            aggregations=GROUP_AGGS,
            transform=lambda df: df.select('piece').head(0),
            batch_size=1,
        )
    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'missing_keys')


def test_output_schema_mismatch_rolls_back(tmp_path, engine, monkeypatch):
    fake_batches(monkeypatch, [
        pl.DataFrame({'value': [1]}),
        pl.DataFrame({'value': ['text']}),
    ])
    destination = tmp_path / 'schema_mismatch'

    with pytest.raises(ValueError, match='Incompatible output schemas'):
        read_database_to_parquet('unused', engine, destination)

    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'schema_mismatch')


def test_transform_exception_after_first_file_cleans_up(tmp_path, engine):
    destination = tmp_path / 'interrupted'
    calls = 0

    def transform(df):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError('broken transform')
        return df

    with pytest.raises(RuntimeError, match='broken transform'):
        read_database_to_parquet(
            PARTS_SQL, engine, destination, batch_size=1, transform=transform
        )

    assert calls == 2
    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'interrupted')


def test_failed_iteration_closes_database_iterator_and_cleans_up(tmp_path, engine, monkeypatch):
    class FailingBatches:
        closed = False

        def __iter__(self):
            yield pl.DataFrame({'value': [1]})
            raise RuntimeError('database read interrupted')

        def close(self):
            self.closed = True

    batches = FailingBatches()
    monkeypatch.setattr(db2parquet.pl, 'read_database', lambda **_kwargs: batches)
    destination = tmp_path / 'partial_read'

    with pytest.raises(RuntimeError, match='database read interrupted'):
        read_database_to_parquet('unused', engine, destination)

    assert batches.closed
    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'partial_read')


def test_write_exception_cleans_up_and_allows_retry(tmp_path, engine, monkeypatch):
    destination = tmp_path / 'write_failure'
    original_write = pl.DataFrame.write_parquet
    attempts = 0

    def failing_write(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise OSError('simulated full disk')
        return original_write(self, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, 'write_parquet', failing_write)
    with pytest.raises(OSError, match='simulated full disk'):
        read_database_to_parquet(PARTS_SQL, engine, destination, batch_size=1)

    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'write_failure')

    monkeypatch.setattr(pl.DataFrame, 'write_parquet', original_write)
    assert read_database_to_parquet(PARTS_SQL, engine, destination, batch_size=2).collect().height == 6


def test_output_part_files_are_sequential(tmp_path, engine):
    destination = tmp_path / 'sequential'
    read_database_to_parquet(PARTS_SQL, engine, destination, batch_size=2)
    files = sorted(path.name for path in destination.glob('part-*.parquet'))

    assert files == [f'part-{i:08d}.parquet' for i in range(len(files))]
    assert len(files) == 3


def test_scan_is_lazy_and_supports_downstream_filters(tmp_path, engine):
    lazy = read_database_to_parquet(PARTS_SQL, engine, tmp_path / 'downstream', batch_size=2)

    assert isinstance(lazy, pl.LazyFrame)
    filtered = lazy.filter(pl.col('doc_id') == 'b').select('piece').collect()
    assert filtered.to_dicts() == [{'piece': 'Pol'}, {'piece': 'ars'}]


def test_grouped_empty_transformed_batches_between_groups(tmp_path, engine):
    """An empty intermediate batch must not flush the unfinished group."""
    destination = tmp_path / 'empty_between'
    result = read_grouped_database_to_parquet(
        PARTS_SQL,
        engine,
        destination,
        group_by='doc_id',
        aggregations=GROUP_AGGS,
        batch_size=1,
        transform=lambda df: df.filter(pl.col('doc_id') != 'b'),
    )
    assert result.collect().to_dicts() == [EXPECTED_GROUPS[0], EXPECTED_GROUPS[2]]


def test_grouped_transform_exception_after_first_write_cleans_up(tmp_path, engine):
    """A grouped extraction must not publish partial aggregates."""
    destination = tmp_path / 'grouped_interrupted'
    calls = 0

    def transform(df):
        nonlocal calls
        calls += 1
        if calls == 5:
            raise RuntimeError('failed while collecting grouped rows')
        return df

    with pytest.raises(RuntimeError, match='failed while collecting grouped rows'):
        read_grouped_database_to_parquet(
            PARTS_SQL,
            engine,
            destination,
            group_by='doc_id',
            aggregations=GROUP_AGGS,
            transform=transform,
            batch_size=1,
        )

    assert calls == 5
    assert not destination.exists()
    assert_no_staging_files(tmp_path, 'grouped_interrupted')
