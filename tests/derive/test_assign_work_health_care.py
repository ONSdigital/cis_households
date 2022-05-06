from chispa import assert_df_equality

from cishouseholds.derive import assign_work_health_care


def test_assign_work_health_care(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", "Yes, in primary care, e.g. GP, dentist", "Yes, primary care, patient-facing"),
            ("No", "Yes, in primary care, e.g. GP, dentist", "Yes, primary care, non-patient-facing"),
            ("No", "No", "No"),
            ("No", None, None),
        ],
        schema="contact string, health_care string, outcome string",
    )
    output_df = assign_work_health_care(
        expected_df.drop("outcome"),
        "outcome",
        "contact",
        "health_care",
    )
    assert_df_equality(output_df, expected_df)
