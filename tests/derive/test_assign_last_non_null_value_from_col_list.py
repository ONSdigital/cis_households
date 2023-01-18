from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.derive import assign_last_non_null_value_from_col_list


def test_assign_last_non_null_value_from_col_list(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2022-01-01", "2022-05-11", "2022-05-11"),
            (1, "2022-09-01", "2022-05-11", "2022-09-01"),
            (2, None, None, None),
            (3, "2022-10-20", None, "2022-10-20"),
            (4, "2022-10-20", "2022-10-20", "2022-10-20"),
            (5, "2022-10-20", None, "2022-10-20"),
        ],
        # fmt: on
        schema="""
        id integer,
        col1 string,
        col2 string,
        result string
        """,
    )
    for col in ["col1", "col2", "result"]:
        expected_df = expected_df.withColumn(col, F.to_timestamp(col, format="yyyy-MM-dd"))

    input_df = expected_df.drop("result")

    output_df = assign_last_non_null_value_from_col_list(
        df=input_df,
        column_name_to_assign="result",
        column_list=["col1", "col2"],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
