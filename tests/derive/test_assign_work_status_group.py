from chispa import assert_df_equality

from cishouseholds.derive import assign_work_status_group


def test_assign_work_status_group(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (None, "Unknown"),
            ("Student", "Student"),
            ("Employed", "Employed"),
            ("Self-employed", "Employed"),
            ("Furloughed (temporarily not working)", "Employed"),
            (
                "Not working (unemployed, retired, long term sick etc)",
                "Not working (unemployed, retired, long term sick etc)",
            ),
        ],
        schema="reference string , group string",
    )
    output_df = assign_work_status_group(
        expected_df.drop("group"),
        "group",
        "reference",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_nullable=True)
