from chispa import assert_df_equality

from cishouseholds.impute import impute_latest_date_flag


def test_impute_latest_date_flag(spark_session):
    expected_data = [
        ("2020-01-01", "2020-01-02", "yes", 1),
    ]
    schema = """
            visit_date string,
            contact_any_covid_date string,
            contact_any_covid string,
            impute_value integer
            """
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    df_input = expected_df.drop("impute_value")

    output_df = impute_latest_date_flag(
        df=df_input,
        window_columns=[
            "visit_date",
            "contact_any_covid_date",
        ],
        imputation_flag_columns="impute_latest_date",
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True)
