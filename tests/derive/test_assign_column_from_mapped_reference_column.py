from chispa import assert_df_equality

from cishouseholds.derive import assign_column_from_mapped_reference_column


def test_assign_work_patient_facing_now(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "Yes"),
            (2, "Yes"),
            (-9, "<=15y"),
            (0, "No"),
        ],
        schema="work integer, facing string",
    )

    output_df = assign_column_from_mapped_reference_column(
        expected_df.drop("facing"), "work", "facing", {-9: "<=15y", -8: ">=75y", 0: "No", 1: "Yes"}, True
    )
    assert_df_equality(output_df, expected_df)
