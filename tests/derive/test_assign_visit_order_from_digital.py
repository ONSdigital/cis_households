from chispa import assert_df_equality

from cishouseholds.derive import assign_visit_order_from_digital


def test_assign_visit_order_from_digital(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Follow-up 1", 1, "2020-07-09"),
            ("Follow-up 2", 1, "2020-07-10"),
            ("First Visit", 1, "2020-07-08"),
            ("First Visit", 2, "2020-07-11"),
            ("Follow-up 1", 2, "2020-07-27"),
        ],
        schema="count string, id integer, date string",
    )
    output_df = assign_visit_order_from_digital(expected_df.drop("count"), "count", "date", "id")
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
