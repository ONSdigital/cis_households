from chispa import assert_df_equality

from cishouseholds.derive import assign_work_patient_facing_now


def test_assign_work_patient_facing_now(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "Yes"),
            (1, "Yes"),
            (-9, "<=15y"),
            (0, "No"),
        ],
        schema="work integer, facing string",
    )

    output_df = assign_work_patient_facing_now(expected_df.drop("facing"), "work", "facing")
    assert_df_equality(output_df, expected_df)
