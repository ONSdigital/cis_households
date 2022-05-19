from chispa import assert_df_equality

from cishouseholds.derive import concat_fields_if_true


def test_concat_fields_if_true(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            ("Yes", "Yes", "Yes", "Yes", "s1s2s3s4"),
            ("Yes", "No", "Yes", "No", "s1s3"),
            ("No", "No", "No", "No", ""),
        ],
        schema="s1 string, s2 string,s3 string,s4 string, output string",
    )
    output_df = concat_fields_if_true(expected_df.drop("output"), "output", "s*", "Yes")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
