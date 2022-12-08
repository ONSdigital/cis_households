from chispa import assert_df_equality

from cishouseholds.edit import normalize_covid_contact_cols


def test_normalise_covid_contact():
    input_df = spark_session.createDataFrame(
        data=[
            ("Yes", None, None),  # correct from yes to no as no date
            ("No", None, None),  # leave as is
            ("No", "2020-01-01", "someone"),  # corrects to Yes as there is a date
            ("No", None, "someone"),  # make type none as no information
            ("Yes", None, "someone"),  # make type and date none and contact No
            ("No", "2020-04-01", None),  # corrects to Yes as there is a date
        ],
        schema="""contact string, contact_date string, contact_type string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("No", None, None),
            ("No", None, None),
            ("Yes", "2020-01-01", "someone"),
            ("No", None, None),
            ("No", None, None),
            ("Yes", "2020-04-01", None),  # corrects to Yes as there is a date
        ],
        schema="""contact string, contact_date string, contact_type string""",
    )
    output_df = normalize_covid_contact_cols(
        input_df, contact_col="contact", contact_date_col="contact_date", contact_type_col="contact_type"
    )
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
