import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import correct_date_ranges
from cishouseholds.edit import correct_date_ranges_union_dependent
from cishouseholds.edit import remove_incorrect_dates


def test_correct_date_ranges(spark_session):
    schema = """visit_date string, date1 string, id integer, key string"""
    input_df = spark_session.createDataFrame(
        data=[
            ("2020-01-01", "2020-07-03", 1, "A"),  # satisfies none
            ("2021-01-01", "2021-01-03", 1, "B"),  # can be shifted back by 1 month
            ("2021-01-01", "2221-01-03", 2, "C"),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 2, "D"),  # valid
            ("2021-01-02", "2022-01-01", 3, "E"),  # can be shifted by a year
            ("2021-01-02", "2022-09-01", 4, "F"),  # set to 2019
            ("2021-02-04", "2221-01-03", 5, "G"),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 5, "H"),  # valid
            ("2021-01-02", "2121-03-03", 5, "I"),  # valid answer pulled from another row
            ("2021-03-01", "2020-03-03", 5, "J"),  # valid
            ("2022-03-01", "2021-03-03", 5, "K"),  # second permissable replacement for C
            ("2021-07-22", "2021-10-11", 6, "L"),  # can be shifted back by a year to 2020
        ],
        schema=schema,
    )
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("2020-01-01", None,         1,"A"),  # satisfies none
            ("2021-01-01", "2020-12-03", 1,"B"),  # can be shifted back by 1 month
            ("2021-01-01", "2020-01-03", 2,"C"),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 2,"D"),  # valid
            ("2021-01-02", "2021-01-01", 3,"E"),  # can be shifted by a year
            ("2021-01-02", "2019-09-01", 4,"F"),  # set to 2019
            ("2021-02-04", "2020-01-03", 5,"G"),  # valid answer pulled from another row
            ("2021-01-01", "2020-01-03", 5,"H"),  # valid answer pulled from another row
            ("2021-01-02", "2020-03-03", 5,"I"),  # valid answer pulled from another row
            ("2021-03-01", "2020-03-03", 5,"J"),  # valid answer pulled from another row
            ("2022-03-01", "2021-03-03", 5,"K"),  # second permissable replacement for C
            ("2021-07-22", "2020-10-11", 6,"L"),  # shifted back a year as >=2020
        ],
        # fmt: on
        schema=schema,
    )
    dfs = [input_df, expected_df]

    for i in range(0, len(dfs)):
        for col in ["visit_date", "date1"]:
            dfs[i] = dfs[i].withColumn(col, F.to_timestamp(col))

    output_df = correct_date_ranges(dfs[0], ["date1"], "visit_date", "2019-08-01")

    output_df = correct_date_ranges_union_dependent(output_df, ["date1"], "id", "visit_date")

    output_df = remove_incorrect_dates(output_df, ["date1"], "visit_date", "2019-08-01")

    assert_df_equality(output_df, dfs[1], ignore_column_order=True, ignore_row_order=True)
