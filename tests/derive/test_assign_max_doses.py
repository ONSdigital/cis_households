from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_max_doses


def test_assign_max_doses(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1, "2020-01-19", "No"),
            (1, 1, 3, "2020-04-06", "Yes"),
        ],
        schema="id integer,idose int, num_doses int, visit_datetime string, max_doses string",
    )
    for col in ["visit_datetime"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))
    output_df = assign_max_doses(
        df=expected_df.drop("max_doses"),
        column_name_to_assign="max_doses",
        i_dose_column="idose",
        num_doses_column="num_doses",
        participant_id_column="id",
        visit_datetime_column="visit_datetime",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
