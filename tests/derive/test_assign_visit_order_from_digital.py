from chispa import assert_df_equality

from cishouseholds.derive import assign_visit_order


def test_assign_visit_order(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2020-07-09", 2),
            (1, "2020-07-10", 3),
            (1, "2020-07-08", 1),
            (2, "2020-07-11", 1),
            (2, "2020-07-27", 2),
        ],
        schema="id integer, date string, count_value integer",
    )

    output_df = assign_visit_order(expected_df.drop("count_value"), "count_value", "date", "id")
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
