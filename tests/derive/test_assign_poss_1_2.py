from chispa import assert_df_equality

from cishouseholds.derive import assign_poss_1_2


def test_assign_poss_1_2(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (1, 1,    "2020-01-01", "Yes"),
            (1, None, "2020-01-02", "Yes"),
            (1, 2,    "2020-01-03", "Yes"),
            (1, None, "2020-01-04", "Yes"),
            (1, None, "2020-01-05", "Yes"),
            (1, 3,    "2020-01-06", "No"),
            (1, None, "2020-01-07", "No"),

            (2, 4,    "2020-01-01", "Yes"),
            (2, None, "2020-01-02", "Yes"),
            (2, 1,    "2020-01-03", "Yes"),
            (2, None, "2020-01-04", "No"),
            (2, 4,    "2020-01-05", "No"),
            (2, 3,    "2020-01-06", "No"),

            (3, 3,    "2020-01-01", "No"),

            (4, 1,    "2020-01-01", "Yes"),
            (4, 2,    "2020-01-02", "Yes"),

            (5, None, "2020-01-01", "Yes"),
            (5, None, "2020-01-02", "Yes"),
        ],
        schema="id integer, num_doses int, visit_datetime string,  pos_1_2 string",
        # fmt: on
    )
    output_df = assign_poss_1_2(
        df=expected_df.drop("pos_1_2"),
        column_name_to_assign="pos_1_2",
        num_doses_column="num_doses",
        visit_datetime_column="visit_datetime",
        participant_id_column="id",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
