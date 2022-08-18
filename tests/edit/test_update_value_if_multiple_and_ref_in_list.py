from chispa import assert_df_equality

from cishouseholds.edit import update_value_if_multiple_and_ref_in_list


def test_update_value_if_multiple_and_ref_in_list(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("ignore;hello;goodbye"),
            ("hello;goodbye;this"),
            ("ignore"),
            ("hello;goodbye"),
            ("this"),
            (""),
            ("hello"),
            ("goodbye"),
        ],
        schema="""col""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("let's ignore"),
            ("let's ignore"),
            ("ignore"),
            ("let's keep"),
            ("this"),
            (""),
            ("hello"),
            ("goodbye"),
        ],
        schema="""col integer""",
    )

    output_df = update_value_if_multiple_and_ref_in_list(
        input_df, "col", ["ignore", "this"], "let's ignore", "let's keep", ";"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
