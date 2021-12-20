from chispa import assert_df_equality

from cishouseholds.derive import assign_grouped_variable_from_days_since


def test_assign_grouped_variable_from_days_since(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", 7, "0-14"),
            ("Yes", 20, "15-28"),
            ("Yes", 30, "29-60"),
            ("Yes", 70, "61-90"),
            ("Yes", 100, "91+"),
            ("Yes", None, "Date not given"),
            ("Don't care", 10, "0-14"),
            ("No", None, None),
        ],
        schema="think_had_covid string, \
                days_since_think_had_covid integer, \
                days_since_think_had_covid_group string",
    )
    input_df = expected_df.drop("days_since_think_had_covid_group")

    output_df = assign_grouped_variable_from_days_since(
        df=input_df,
        binary_reference_column="think_had_covid",
        days_since_reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
