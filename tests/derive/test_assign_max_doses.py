from chispa import assert_df_equality

from cishouseholds.derive import assign_max_doses


def test_assign_max_doses(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 1, 1, "Yes"),
            (1, 1, 3, "Yes"),
        ],
        schema="id integer,idose int, num_doses int, max_doses string",
    )
    output_df = assign_max_doses(
        df=expected_df.drop("max_doses"),
        column_name_to_assign="max_doses",
        i_dose_column="idose",
        num_doses_column="num_doses",
        participant_id_column="id",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
