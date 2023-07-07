from chispa import assert_df_equality

from cishouseholds.derive import assign_condition_around_event


def test_assign_condition_around_event(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "No", 1, "2020-07-20", "Yes"),
            (2, "No", 1, "2020-07-20", "No"),
            (3, "Yes", 2, "2020-02-18", "Yes"),
            (1, "Yes", 2, "2020-08-20", "Yes"),
            (2, "No", 3, "2020-08-20", "Yes"),
            (3, "No", 3, "2020-03-18", "Yes"),
            (1, "Yes", 3, "2020-09-20", "Yes"),
            (2, "Yes", 4, "2020-09-20", "Yes"),
            (3, "No", 4, "2020-04-18", "No"),
        ],
        schema="id integer, symptoms string, visit_id integer, visit_date string, result string",
    )
    output_df = assign_condition_around_event(
        df=expected_df.drop("result"),
        column_name_to_assign="result",
        condition_bool_column="symptoms",
        id_column="id",
        event_date_column="visit_date",
        event_id_column="visit_id",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
