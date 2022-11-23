from chispa import assert_df_equality

from cishouseholds.derive import assign_work_person_facing_now


def test_assign_work_person_facing_now(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("<=15y", 15, "Yes, care/residential home, resident-facing", "<=15y"),
            ("Yes", 27, "Yes, care/residential home, resident-facing", "Yes"),
            ("No", 49, "Yes, other social care, resident-facing", "Yes"),
            ("No", 33, "No", "No"),
            (">=75y", 80, "No", ">=75y"),
            (None, 80, "No", ">=75y"),
            (">=75y", 99, "Yes, care/residential home, non-resident-facing", ">=75y"),
        ],
        schema="work_patient string, age integer, work_social string, facing string",
    )
    output_df = assign_work_person_facing_now(
        df=expected_df.drop("facing"),
        column_name_to_assign="facing",
        age_column="age",
        work_patient_facing_now_column="work_patient",
        work_social_care_column="work_social",
    )
    assert_df_equality(output_df, expected_df)
