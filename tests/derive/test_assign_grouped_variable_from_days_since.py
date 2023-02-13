from chispa import assert_df_equality

from cishouseholds.derive import assign_grouped_variable_from_days_since


def test_assign_grouped_variable_from_days_since(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", 7, "1"),
            ("Yes", 20, "2"),
            ("Yes", 30, "3"),
            ("Yes", 70, "4"),
            ("Yes", 100, "5"),
            ("Yes", None, "6"),
            ("Don't care", 10, "1"),
            ("No", None, "6"),
        ],
        schema="contact_known_or_suspected_covid string, \
                contact_known_or_suspected_covid_days_since integer, \
                contact_known_or_suspected_covid_days_since_group string",
    )
    input_df = expected_df.drop("contact_known_or_suspected_covid_days_since_group")

    output_df = assign_grouped_variable_from_days_since(
        df=input_df,
        binary_reference_column="contact_known_or_suspected_covid",
        days_since_reference_column="contact_known_or_suspected_covid_days_since",
        column_name_to_assign="contact_known_or_suspected_covid_days_since_group",
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
