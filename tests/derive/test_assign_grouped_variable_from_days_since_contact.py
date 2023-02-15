from chispa import assert_df_equality

from cishouseholds.derive import assign_grouped_variable_from_days_since_contact


def test_assign_grouped_variable_from_days_since_contact(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Living in your own home", 7, "1"),
            ("Outside your home", 20, "2"),
            ("Someone you do not live with", 30, "3"),
            ("Someone you live with", 70, "4"),
            ("Someone you live with", 100, "5"),
            ("Someone you live with", None, "6"),
            (None, 10, "1"),
        ],
        schema="contact_known_or_suspected_covid string, \
                contact_known_or_suspected_covid_days_since integer, \
                contact_known_or_suspected_covid_days_since_group string",
    )
    input_df = expected_df.drop("days_since_think_had_covid_group")

    output_df = assign_grouped_variable_from_days_since_contact(
        df=input_df,
        reference_column="contact_known_or_suspected_covid",
        days_since_reference_column="contact_known_or_suspected_covid_days_since",
        column_name_to_assign="contact_known_or_suspected_covid_days_since_group",
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
