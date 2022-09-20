from chispa import assert_df_equality

from cishouseholds.derive import assign_column_value_from_multiple_column_map


def test_assign_column_value_from_multiple_column_map(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("No", None, "No"),
            ("Yes", None, None),
            ("No", "Primary", "Yes, primary care, non-patient-facing"),
            ("Yes", "Other", "Yes, other healthcare, patient-facing"),
        ],
        schema="is_patient_facing string, healthcare_area string, result string",
    )

    output_df = assign_column_value_from_multiple_column_map(
        expected_df.drop("result"),
        "result",
        [
            ["No", ["No", None]],
            ["Yes, primary care, patient-facing", ["Yes", "Primary"]],
            ["Yes, secondary care, patient-facing", ["Yes", "Secondary"]],
            ["Yes, other healthcare, patient-facing", ["Yes", "Other"]],
            ["Yes, primary care, non-patient-facing", ["No", "Primary"]],
            ["Yes, secondary care, non-patient-facing", ["No", "Secondary"]],
            ["Yes, other healthcare, non-patient-facing", ["No", "Other"]],
        ],
        ["is_patient_facing", "healthcare_area"],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)
