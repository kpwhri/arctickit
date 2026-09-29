import polars as pl

from arctickit.expr import (
    all_columns_non_null_expr,
    any_flag_column_true_expr,
    classification_indicator_exprs,
)


def test_all_columns_non_null_expr_returns_true_only_when_all_columns_are_non_null():
    df = pl.DataFrame({
        'a': [1, 1, None, None],
        'b': [2, None, 2, None],
        'c': ['x', 'y', 'z', None],
    })

    result = df.select(
        all_columns_non_null_expr('a', 'b', 'c').alias('all_present')
    )

    assert result['all_present'].to_list() == [True, False, False, False]


def test_all_columns_non_null_expr_can_be_used_to_filter_rows():
    df = pl.DataFrame({
        'id': [1, 2, 3, 4],
        'start': ['2026-01-01', '2026-01-01', None, '2026-01-01'],
        'end': ['2026-01-31', None, '2026-01-31', '2026-01-31'],
    })

    result = df.filter(
        all_columns_non_null_expr('start', 'end'),
    )

    assert result['id'].to_list() == [1, 4]


def test_any_flag_column_true_expr_returns_true_when_any_flag_is_truthy():
    df = pl.DataFrame({
        'flag_a': [0, 1, 0, 0],
        'flag_b': [0, 0, 1, 0],
        'flag_c': [0, 0, 0, 0],
    })

    result = df.select(
        any_flag_column_true_expr('flag_a', 'flag_b', 'flag_c').alias('any_flag'),
    )

    assert result['any_flag'].to_list() == [False, True, True, False]


def test_any_flag_column_true_expr_casts_numeric_flags_to_boolean():
    df = pl.DataFrame({
        'flag_a': [0, 2, 0],
        'flag_b': [0, 0, -1],
    })

    result = df.select(
        any_flag_column_true_expr('flag_a', 'flag_b').alias('any_flag'),
    )

    assert result['any_flag'].to_list() == [False, True, True]


def test_classification_indicator_exprs_creates_one_indicator_per_value():
    df = pl.DataFrame({
        'encounter_id': [1, 1, 1, 2, 2, 3],
        'classification': ['diabetes', 'copd', 'diabetes', 'renal', 'copd', 'other'],
    })

    result = (
        df.group_by('encounter_id')
        .agg(
            classification_indicator_exprs(
                'classification',
                ('diabetes', 'copd', 'renal'),
            )
        )
        .sort('encounter_id')
    )

    assert result.select(['diabetes', 'copd', 'renal']).to_dict(as_series=False) == {
        'diabetes': [1, 0, 0],
        'copd': [1, 1, 0],
        'renal': [0, 1, 0],
    }


def test_classification_indicator_exprs_outputs_int8_columns():
    df = pl.DataFrame({
        'encounter_id': [1, 1, 2],
        'classification': ['a', 'b', 'c'],
    })

    result = df.group_by('encounter_id').agg(
        classification_indicator_exprs('classification', ('a', 'b')),
    )

    assert result.schema['a'] == pl.Int8
    assert result.schema['b'] == pl.Int8
