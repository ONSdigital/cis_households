from chispa import assert_df_equality

from cishouseholds.derive import assign_unique_id_column


def test_assign_unique_id_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[("XAE-12", "XAE", "12"), ("BSE-53", "BSE", "53"), ("53", None, "53")],
        schema=["id", "A", "B"],
    )

    input_df = expected_df.drop("id")

    output_df = assign_unique_id_column(input_df, "id", ["A", "B"])

    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_nullable=True)
