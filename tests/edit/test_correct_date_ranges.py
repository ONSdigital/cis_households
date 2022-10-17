from chispa import assert_df_equality

from cishouseholds.edit import correct_date_ranges
import pyspark.sql.functions as F

def test_convert_null_if_not_in_list(spark_session):
    schema="""visit_date string, date1 string, id integer"""
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-01-03", 1),
            ("2020-01-02", "2020-01-02", 1),
        ],
        schema=schema
    )
    input_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-01-01", 1),
            ("2020-01-02", "2020-01-01", 1),
        ],
        schema=schema
    )
    for df in [input_df,expected_df]:
        for col in ["visit_date","date1"]:
            df = df.withColumn(col,F.to_timestamp(col))
        df.show()

    output_df = correct_date_ranges(input_df, ["date1"],"visit_date", 2019)
    assert_df_equality(output_df, expected_df)
