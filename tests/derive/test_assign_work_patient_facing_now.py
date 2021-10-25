from chispa import assert_df_equality

from cishouseholds.derive import assign_work_patient_facing_now


def test_assign_work_patient_facing_now(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, "No", "<=15y"),
            (15, "Yes", "<=15y"),
            (55, "Yes, primary care, patient-facing", "Yes"),
            (69, "Yes, other healthcare, patient-facing", "Yes")(49, "No", "No"),
            (75, "No", ">=75y"),
        ],
        schema="age integer, work_health string, facing string",
    )
    output_df = assign_work_patient_facing_now(expected_df.drop("facing"), "facing", "age", "work_health")
    assert_df_equality(output_df, expected_df)
