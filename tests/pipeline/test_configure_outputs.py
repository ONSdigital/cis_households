import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.generate_outputs import configure_outputs


def test_configure_outputs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("England", 6, 2, "02-6SY"),
            ("NI", 9, 5, "02-6SY"),
            ("Scotland", 11, 7, "07SY-11SY"),
            ("Wales", 15, 10, "07SY-11SY"),
            ("Wales", 15, 6, "07SY-11SY"),
            ("Scotland", 15, 6, None),
            ("England", 17, 12, "12SY-24"),
            ("NI", 18, 13, "12SY-24"),
            ("England", 25, 12, "25-34"),
            ("NI", 55, 79, "50-69"),
            ("NI", 88, 1, "70+"),
        ],
        schema="country string, age integer, school_year integer, output string",
    )
    expected_df1 = spark_session.createDataFrame(
        data=[
            ("England", 6, 2, "trumpet"),
            ("NI", 9, 5, "trumpet"),
            ("Scotland", 11, 7, "07SY-11SY"),
            ("Wales", 15, 10, "07SY-11SY"),
            ("Wales", 15, 6, "07SY-11SY"),
            ("Scotland", 15, 6, None),
            ("England", 17, 12, "12SY-24"),
            ("NI", 18, 13, "12SY-24"),
            ("England", 25, 12, "25-34"),
            ("NI", 55, 79, "50-69"),
            ("NI", 88, 1, "gibberish"),
        ],
        schema="country string, age integer, renamed integer, output string",
    )
    expected_df2 = spark_session.createDataFrame(
        data=[
            ("Wales", 2),
            ("NI", 4),
            ("England", 3),
            ("Scotland", 2),
        ],
        schema="country string, test long",
    )
    expected_df3 = spark_session.createDataFrame(
        data=[
            (3, 2),
            (1, 4),
            (2, 3),
            (4, 2),
        ],
        schema="country string, test long",
    )
    # test mapping functionality with complete map off
    output_df1 = configure_outputs(
        input_df,
        selection_columns=["country", "age", "school_year", "output"],
        name_map={"school_year": "renamed"},
        value_map={"output": {"70+": "gibberish", "02-6SY": "trumpet"}},
    )
    output_df5 = configure_outputs(
        input_df,
        selection_columns=["country", "age", "school_year"],
        group_by_columns="country",
        value_map={"county": {"NI": 1, "England": 2, "Wales": 3, "Scotland": 4}},
        complete_map=True,
    )
    # test correct grouping functionality
    output_df2 = configure_outputs(
        input_df, group_by_columns="country", aggregate_function="count", aggregate_column_name="test"
    )

    assert_df_equality(output_df1, expected_df1, ignore_nullable=True)
    assert_df_equality(output_df2, expected_df2, ignore_nullable=True)
    assert_df_equality(output_df5, expected_df3, ignore_nullable=True)

    # test function dissalows using functions on non-selected columns
    with pytest.raises(IndexError):
        configure_outputs(input_df, group_by_columns="country", selection_columns="age")

    # test function raises readable error for column not existing on dataframe
    with pytest.raises(AttributeError):
        configure_outputs(input_df, selection_columns="nothing")
