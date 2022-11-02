import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import correct_date_ranges


def test_correct_date_ranges(spark_session):
    schema = """visit_date string, date1 string, id integer"""
    input_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-07-03", 1),  # satisfies none
            ("2021-01-01", "2021-01-03", 1),  # can be shifted back by 1 month
            ("2021-01-01", "2221-01-03", 2),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 2),  # valid
            ("2021-01-02", "2022-01-01", 3),  # can be shifted by a year
            ("2021-01-02", "2022-09-01", 4),  # set to 2019
            ("2021-02-04", "2221-01-03", 5),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 5),  # valid
            ("2021-01-02", "2121-03-03", 5),  # valid answer pulled from another row
            ("2021-03-01", "2020-03-03", 5),  # valid
            ("2021-07-22", "2021-10-11", 6),  # can be shifted back by a year to 2020
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", None, 1),  # satisfies none
            ("2021-01-01", "2020-12-03", 1),  # can be shifted back by 1 month
            ("2021-01-01", "2020-01-03", 2),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 2),  # valid
            ("2021-01-02", "2021-01-01", 3),  # can be shifted by a year
            ("2021-01-02", "2019-09-01", 4),  # set to 2019
            ("2021-02-04", "2020-01-03", 5),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 5),  # valid answer pulled from another row
            ("2021-01-02", "2020-03-03", 5),  # valid answer pulled from another row
            ("2021-03-01", "2020-03-03", 5),  # valid answer pulled from another row
            ("2021-07-22", "2020-10-11", 6),  # shifted back a year as >=2020
        ],
        schema=schema,
    )
    dfs = [input_df, expected_df]

    for i in range(0, len(dfs)):
        for col in ["visit_date", "date1"]:
            dfs[i] = dfs[i].withColumn(col, F.to_timestamp(col))

    output_df = correct_date_ranges(dfs[0], ["date1"], "id", "visit_date", "2019-08-01")

    assert_df_equality(output_df, dfs[1], ignore_column_order=True, ignore_row_order=True)
