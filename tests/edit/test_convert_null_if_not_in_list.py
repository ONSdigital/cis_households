from chispa import assert_df_equality

from cishouseholds.edit import convert_null_if_not_in_list


def test_convert_null_if_not_in_list(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("male", 1), ("female", 2), (None, 3), (None, 4)],
        schema="""sex string, id integer""",
    )
    input_df = spark_session.createDataFrame(
        data=[("male", 1), ("female", 2), ("helicopter", 3), ("dont know", 4)],
        schema="""sex string, id integer""",
    )
    output_df = convert_null_if_not_in_list(input_df, "sex", ["male", "female"])
    assert_df_equality(output_df, expected_df)
