import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.impute import impute_think_had_covid


def test_impute_think_had_covid(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # 0 - Check carry forward of first records date, type and contact when no contact reported on later visit
            (0, "2020-01-01", "2020-01-01", "A", "Yes"),  # test fill forward date once
            (0, None, "2020-01-01", None, "No"),
            (1, "2020-03-12", "2020-12-20", "A", "Yes"),  # test carry forward from visit after first
            (1, None, "2020-11-20", "B", "Yes"),
            (1, "2020-02-02", "2020-01-20", "B", "Yes"),
            (2, None, "2020-11-20", "A", "Yes"),
            (2, "2020-04-05", "2020-12-20", None, "Yes"),
            (3, "2020-05-09", "2020-12-20", "B", "Yes"),  # test no imputation cases
            (4, None, "2020-10-20", None, "No"),  # test type prioritization
            (4, "2020-11-09", "2020-11-20", None, "Yes"),
            (4, "2020-11-22", "2021-01-11", "B", "Yes"),
            (4, None, "2021-02-11", "A", "Yes"),
        ],
        schema="id integer, date string, visit_date string, type string, contact string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            (0, None, "2020-01-01", "A", "No"),
            (0, None, "2020-01-01", "A", "No"),
            (1, None, "2020-01-20", "B", "No"),
            (1, "2020-03-12", "2020-12-20", "A", "Yes"),
            (1, "2020-03-12", "2020-11-20", "B", "Yes"),
            (2, "2020-04-05", "2020-11-20", "A", "Yes"),
            (2, "2020-04-05", "2020-12-20", None, "Yes"),
            (3, "2020-05-09", "2020-12-20", "B", "Yes"),
            (4, None, "2020-10-20", None, "No"),
            (4, "2020-11-09", "2020-11-20", None, "Yes"),
            (4, "2020-11-22", "2021-01-11", "B", "Yes"),
            (4, "2020-11-22", "2021-02-11", "A", "Yes"),
        ],
        schema="id integer, date string, visit_date string, type string, contact string",
    )
    for col in ["date", "visit_date"]:
        input_df = input_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))
        expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))

    output_df = impute_think_had_covid(input_df, "id", "date", "visit_date", "type", "contact", {"A": 1, "B": 2})
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
