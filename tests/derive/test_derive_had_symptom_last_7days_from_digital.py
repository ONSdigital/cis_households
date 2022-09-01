from chispa import assert_df_equality

from cishouseholds.derive import derive_had_symptom_last_7days_from_digital


def test_derive_had_symptom_last_7days_from_digital(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", "Yes", "Yes", "No"),
            ("No", "No", "No", "No"),
            ("Yes", None, "Yes", "No"),
            (None, None, None, None),
            ("No", None, "No", None),
        ],
        schema="result string, s1 string, s2 string, s3 string",
    )
    output_df = derive_had_symptom_last_7days_from_digital(expected_df.drop("result"), "result", "s", [1, 2, 3])
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_column_order=True)
