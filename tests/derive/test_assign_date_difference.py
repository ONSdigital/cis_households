import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.derive import assign_date_difference


def test_assign_any_symptoms_around_visit(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2020-07-20", "2020-07-29", 9),
            (2, "2020-07-20", "2020-08-12", 3),
            (3, None, "2020-08-12", 93),
        ],
        schema="id integer, date_1 string, date_2 string, diff integer",
    )
    output_df1 = assign_date_difference(
        df=expected_df.drop("ref").filter(F.col("id") == 1),
        column_name_to_assign="diff",
        start_reference_column="date_1",
        end_reference_column="date_2",
    )
    output_df2 = assign_date_difference(
        df=expected_df.drop("ref").filter(F.col("id") == 2),
        column_name_to_assign="diff",
        start_reference_column="date_1",
        end_reference_column="date_2",
        format="weeks",
    )
    output_df3 = assign_date_difference(
        df=expected_df.drop("ref").filter(F.col("id") == 3),
        column_name_to_assign="diff",
        start_reference_column="survey start",
        end_reference_column="date_2",
    )
    assert_df_equality(output_df1, expected_df.filter(F.col("id") == 1), ignore_nullable=True, ignore_row_order=True)
    assert_df_equality(output_df2, expected_df.filter(F.col("id") == 2), ignore_nullable=True, ignore_row_order=True)
    assert_df_equality(output_df3, expected_df.filter(F.col("id") == 3), ignore_nullable=True, ignore_row_order=True)
