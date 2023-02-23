from chispa import assert_df_equality

from cishouseholds.derive import group_participant_within_date_range


def test_group_participant_within_date_range(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "2021-01-23", 1),
            (1, "2021-02-01", 1),
            (1, "2021-03-21", 2),
            (2, "2021-01-23", 1),
            (2, "2021-03-01", 2),
            (2, "2021-05-21", 3),
        ],
        schema="id integer,date string, group integer",
    )

    output_df = group_participant_within_date_range(
        df=expected_df.drop("group"),
        column_name_to_assign="group",
        date_column="date",
        date_range=15,
        participant_id_column="id",
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True)
