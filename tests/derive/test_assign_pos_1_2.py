from chispa import assert_df_equality

from cishouseholds.derive import assign_pos_1_2


def test_assign_pos_1_2(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1, "Yes"),
            (1, 1, 3, "No"),
        ],
        schema="id integer,idose int, num_doses int, pos_1_2 string",
    )
    output_df = assign_pos_1_2(
        df=expected_df.drop("pos_1_2"),
        column_name_to_assign="pos_1_2",
        i_dose_column="idose",
        num_doses_column="num_doses",
        participant_id_column="id",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
