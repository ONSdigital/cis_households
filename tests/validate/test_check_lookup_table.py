import pytest

from cishouseholds.validate import check_lookup_table_joined_columns_unique
from cishouseholds.validate import check_lookup_table_not_complete_duplicates


def test_check_lookup_table_joined_columns_unique_fail(spark_session):
    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',      'b',     'c',     'd'),
                ('a',      'b',     'f',     'k'), # join on columns duplicate
                ('a',      'b',     'c',     'd'),
            # complete duplicate
            # fmt: on
        ],
        schema="""
               key_1 string,
               key_2 string,
               col_1 string,
               col_2 string
            """,
    )
    with pytest.raises(ValueError) as lookup_exception:
        check_lookup_table_joined_columns_unique(df=df_input, join_column_list=["key_1", "key_2"])

    assert (
        "The lookup dataframe  used for joining on key_1, key_2 columns have duplicated entries that should be unique. These are the rows:"
        in str(lookup_exception.value)
    )


def test_check_lookup_table_joined_columns_unique_pass(spark_session):
    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',      'b',     'c',     'd'),
                ('a',      'x',     'f',     'k'), # join on columns duplicate
                ('a',      'y',     'c',     'd'),
            # complete duplicate
            # fmt: on
        ],
        schema="""
               key_1 string,
               key_2 string,
               col_1 string,
               col_2 string
            """,
    )
    check_lookup_table_joined_columns_unique(df=df_input, join_column_list=["key_1", "key_2"])


def test_check_lookup_table_not_complete_duplicates_pass(spark_session):
    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',      'b',     'c',     'd'),
                ('a',      'x',     'f',     'k'),
            # rows different
            # fmt: on
        ],
        schema="""
               key_1 string,
               key_2 string,
               col_1 string,
               col_2 string
            """,
    )
    check_lookup_table_not_complete_duplicates(df=df_input)


def test_check_lookup_table_not_complete_duplicates_fail(spark_session):
    df_input = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('a',      'b',     'c',     'd'),
                ('a',      'b',     'c',     'd'),
            # complete duplicate
            # fmt: on
        ],
        schema="""
               key_1 string,
               key_2 string,
               col_1 string,
               col_2 string
            """,
    )
    with pytest.raises(ValueError) as lookup_exception:
        check_lookup_table_not_complete_duplicates(df=df_input)

    assert "The lookup dataframe  has complete duplicated entries that should be unique. These are the rows:" in str(
        lookup_exception.value
    )
