from chispa import assert_df_equality

from cishouseholds.impute import impute_visit_datetime


def test_impute_visit_datetime(spark_session):

    expected_data = [("20/01/2021", "20/01/2021"), ("20/01/2021", "25/06/2021"), (None, None)]

    input_data = [(None, "20/01/2021"), ("20/01/2021", "25/06/2021"), (None, None)]

    schema = "visit_datetime string, sampled_datetime string"

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)

    actual_df = impute_visit_datetime(input_df, "visit_datetime", "sampled_datetime")

    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
