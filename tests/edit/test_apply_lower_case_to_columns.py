from chispa import assert_df_equality

from cishouseholds.edit import update_strings_to_sentence_case


def test_update_strings_to_sentence_case(spark_session):

    input_df = spark_session.createDataFrame(
        data=[
            ("good MORning", "word soup"),
            ("HELLO THERE", "whATs UP"),
            ("WELL WELL well", "WeLL then"),
        ],
        schema="""col1 string, col2 string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("Good morning", "Word soup"),
            ("Hello there", "Whats up"),
            ("Well well well", "Well then"),
        ],
        schema="""col1 string, col2 string""",
    )

    columns_list = ["col1", "col2"]

    output_df = update_strings_to_sentence_case(input_df, columns_list)

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
