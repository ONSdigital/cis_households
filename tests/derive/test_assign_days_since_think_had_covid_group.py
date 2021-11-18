from chispa import assert_df_equality

from cishouseholds.derive import assign_days_since_think_had_covid_group


def test_assign_days_since_think_had_covid_group(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("yes", 7, "0-14"),
            ("yes", 20, "15-28"),
            ("yes", 30, "29-60"),
            ("yes", 70, "61-90"),
            ("yes", 100, "91+"),
            ("Yes", None, "Date not given"),
            ("no", 6, None),
            ("no", 10, None),
            ("no", 10, None),
            ("no", 10, None),
        ],
        schema="think_had_covid string, \
                days_since_think_had_covid integer, \
                days_since_think_had_covid_group string",
    )
    input_df = expected_df.drop("days_since_think_had_covid_group")

    output_df = assign_days_since_think_had_covid_group(
        df=input_df,
        think_had_covid_column="think_had_covid",
        reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
