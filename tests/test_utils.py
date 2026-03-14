import polars as pl

from arctickit.utils import lowercase_columns, remove_nbsp


def test_lowercase_columns_renames_all_columns_to_lowercase():
    df = pl.DataFrame(
        {
            'ILMARINEN': [1, 2],
            'SeppoName': ['a', 'b'],
            'Already_Lower': [True, False],
        }
    )

    out_df = lowercase_columns(df)

    assert out_df.columns == ['ilmarinen', 'sepponame', 'already_lower']
    assert out_df.to_dict(as_series=False) == {
        'ilmarinen': [1, 2],
        'sepponame': ['a', 'b'],
        'already_lower': [True, False],
    }


def test_remove_nbsp_strips_trailing_whitespace_from_string_columns_only():
    df = pl.DataFrame(
        {
            'hero': ['Väinämöinen   ', 'Ilmarinen\t', 'Lemminkäinen'],
            'place': ['Pohjola  ', 'Kalevala', 'Tuonela   '],
            'count': [1, 2, 3],
        }
    )

    out_df = remove_nbsp(df)

    assert out_df['hero'].to_list() == ['Väinämöinen', 'Ilmarinen', 'Lemminkäinen']
    assert out_df['place'].to_list() == ['Pohjola', 'Kalevala', 'Tuonela']
    assert out_df['count'].to_list() == [1, 2, 3]
