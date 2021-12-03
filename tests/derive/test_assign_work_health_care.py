from chispa import assert_df_equality

from cishouseholds.derive import assign_work_health_care


def test_assign_work_health_care(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", None, "Yes, in primary care, e.g. GP, dentist", "Yes, primary care, patient-facing"),
            ("No", None, "Yes, in primary care, e.g. GP, dentist", "Yes, primary care, non-patient-facing"),
            ("No", None, "No", "No"),
            ("No", "Original value", None, "Original value"),
        ],
        schema="contact string, health_care string, health_care_other string, combined string",
    )
    output_df = assign_work_health_care(
        expected_df.drop("combined"), "combined", "contact", "health_care", "health_care_other"
    )
    assert_df_equality(output_df, expected_df)
