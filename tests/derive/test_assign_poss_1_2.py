from chispa import assert_df_equality

from cishouseholds.derive import assign_poss_1_2


def test_assign_poss_1_2(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[(1, 1, "Yes"), (1, 3, "Yes"), (1, 2, "Yes"), (1, 3, "No")],
        schema="id integer, num_doses int, pos_1_2 string",
    )
    output_df = assign_poss_1_2(
        df=expected_df.drop("pos_1_2"),
        column_name_to_assign="pos_1_2",
        num_doses_column="num_doses",
        participant_id_column="id",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
