from chispa import assert_df_equality

from cishouseholds.derive import assign_fake_id


def test_assign_fake_id(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("A", 1), ("A", 1), ("D", 4), ("C", 3), ("B", 2), ("D", 4)],
        schema="id string, fake_id integer",
    )
    output_df = assign_fake_id(df=expected_df.drop("fake_id"), column_name_to_assign="fake_id", reference_column="id")
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
