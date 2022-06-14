from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_datetime_from_group


def test_assign_any_symptoms_around_visit(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-04-18", "2020-04-18 11:59:59", "2020-07-20 05:30:00", "18-04-2020 12:00:00"),
            ("2020-04-18", "2020-04-18 11:59:59", "2020-07-20 15:59:59", "18-04-2020 12:00:00"),
            ("2020-04-18", "2020-04-18 11:59:59", "2020-04-18 00:00:00", "18-04-2020 12:00:00"),
        ],
        schema="date_1 string, date_2 string, date_3 string, result string",
    )
    output_df = assign_datetime_from_group(
        expected_df.drop("result"),
        column_name_to_assign="result",
        ordered_columns=["date_1", "date_2", "date_3"],
        date_format="dd-MM-yyyy",
        time_format="HH:mm:ss",
        default_timestamp="12:00:00",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
