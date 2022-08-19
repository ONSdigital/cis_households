from chispa import assert_df_equality

from cishouseholds.edit import update_value_if_multiple_and_ref_in_list


def test_update_value_if_multiple_and_ref_in_list(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            (1, "ignore;hello;goodbye"),
            (2, "hello;goodbye;this"),
            (3, "ignore"),
            (4, "hello;goodbye"),
            (5, "this"),
            (6, ""),
            (7, "hello"),
            (8, "goodbye"),
        ],
        schema="""id integer, col string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            (1, "let's ignore"),
            (2, "let's ignore"),
            (3, "ignore"),
            (4, "let's keep"),
            (5, "this"),
            (6, ""),
            (7, "hello"),
            (8, "goodbye"),
        ],
        schema="""id integer, col string""",
    )

    output_df = update_value_if_multiple_and_ref_in_list(
        input_df, "col", ["ignore", "this"], "let's ignore", "let's keep", ";"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
