from chispa import assert_df_equality

from cishouseholds.edit import correct_date_ranges


def test_convert_null_if_not_in_list(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-01-01", 1),
            ("2020-01-02", "2020-01-01", 1),
        ],
        schema="""visit_date string, date1 string, id integer""",
    )
    input_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-01-01", 1),
            ("2020-01-02", "2020-01-01", 1),
        ],
    )
    output_df = correct_date_ranges(input_df, "french_cars", ["Renault", "Fiat"])
    assert_df_equality(output_df, expected_df)
