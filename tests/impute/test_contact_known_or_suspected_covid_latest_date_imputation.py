from chispa import assert_df_equality

from cishouseholds.impute import impute_latest_date_flag


def test_impute_latest_date_flag(spark_session):
    expected_data = [
        # fmt:off
            (1,     "2020-01-01",   1, 1,   "2020-01-02",   "2020-01-02"), # case 1
            (1,     "2020-01-01",   1, 1,   None,           "2020-01-03"),
            (1,     "2020-01-01",   1, 1,   None,           "2020-01-03"),

            (1,     "2020-01-01",   2, 1,   "2020-01-03",   "2020-01-03"), # case 2: different visit id
            (1,     "2020-01-01",   2, 1,   None,           "2020-01-03"),

            (1,     "2020-01-01",   3, 1,   None,           "2020-01-03"), # carry contact_any_covid_date

            # another patient contact_any_covid_date should remain the same
            (2,     "2020-01-02",   1, 1,   None,           None),
        # fmt:on
    ]
    schema = """
            participant_id integer,
            visit_date string,
            visit_id integer,
            contact_any_covid integer,
            contact_any_covid_date string,
            contact_any_covid_date_output string
            """
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    input_df = expected_df.drop("contact_any_covid_date_output")
    expected_df = expected_df.drop("contact_any_covid_date").withColumnRenamed(
        "contact_any_covid_date_output", "contact_any_covid_date"
    )

    output_df = impute_latest_date_flag(
        df=input_df,
        participant_id_column="participant_id",
        visit_date_column="visit_date",
        visit_id_column="visit_id",
        contact_any_covid_column="contact_any_covid",
        contact_any_covid_date_column="contact_any_covid_date",
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
