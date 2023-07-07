import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_incremental_order


def test_assign_incremental_order(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, 1,  "2020-07-09", 2),
            (1, 2,  "2020-07-10", 3),
            (1, 3,  "2020-07-10", 4), # identical patient_id and date but different visit_id
            (1, 4,  "2020-07-08", 1),

            (2, 1,  "2020-07-11", 1),
            (2, 2,  "2020-07-27", 2),
            # fmt: on
        ],
        schema="patient_id integer, visit_id integer, date string, count_value integer",
    )
    expected_df = expected_df.withColumn("date", F.to_timestamp("date"))

    output_df = assign_incremental_order(
        df=expected_df.drop("count_value"),
        column_name_to_assign="count_value",
        id_column="patient_id",
        order_list=["date", "visit_id"],
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
