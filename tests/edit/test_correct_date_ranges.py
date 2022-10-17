import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import correct_date_ranges


def test_convert_null_if_not_in_list(spark_session):
    schema = """visit_date string, date1 string, id integer"""
    input_df = spark_session.createDataFrame(
        data=[
            ("2020-09-01", "2020-12-03", 1),
            ("2021-01-01", "2021-01-03", 1),
            ("2021-01-02", "2022-05-01", 1),
            ("2021-01-02", "2022-01-01", 1),
            ("2021-01-02", "2019-01-01", 1),
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-09-01", "2019-12-03", 1),
            ("2021-01-01", "2020-12-03", 1),
            ("2021-01-02", "2020-05-01", 1),
            ("2021-01-02", "2021-01-01", 1),
            ("2021-01-02", "2021-01-01", 1),
        ],
        schema=schema,
    )
    dfs = [input_df, expected_df]
    for i in range(0, len(dfs)):
        for col in ["visit_date", "date1"]:
            dfs[i] = dfs[i].withColumn(col, F.to_timestamp(col))

    output_df = correct_date_ranges(dfs[0], ["date1"], "visit_date", "2019-08-01")

    assert_df_equality(output_df, dfs[1])
