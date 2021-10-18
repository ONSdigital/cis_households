from chispa import assert_df_equality

from cishouseholds.derive import assign_work_person_facing_now


def test_assign_work_person_facing_now(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("<=15y", 1, "Yes, care/residential home, resident-facing"),
            ("Yes", 0, "No"),
            ("Yes", 2, "Yes, other social care, resident-facing"),
            ("No", 0, "No"),
            (">=75y", 0, "No"),
            (">=75y", 3, "Yes, care/residential home, non-resident-facing"),
        ],
        schema="work_patient string, work_social integer, facing string",
    )
    output_df = assign_work_person_facing_now(expected_df.drop("facing"), "facing", "work_patient", "work_social")
    assert_df_equality(output_df, expected_df)
