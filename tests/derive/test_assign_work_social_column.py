from chispa import assert_df_equality

from cishouseholds.derive import assign_work_social_column


def test_assign_work_social_column(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (None, None, "No", "Yes"),
            ("No", "Self-employed", "No", "Yes"),
            ("No", "Self-employed", None, None),
            ("Yes, care/residential home, resident-facing", "Social care", "Yes", "Yes"),
            ("Yes, other social care, resident-facing", "Social care", "No", "Yes"),
            ("Yes, other social care, resident-facing", "Social care", None, "Yes"),
            ("Yes, care/residential home, non-resident-facing", "Social care", "Yes", None),
            ("Yes, care/residential home, non-resident-facing", "Social care", "Yes", "No"),
            ("Yes, other social care, non-resident-facing", "Social care", "No", "No"),
            ("Yes, other social care, non-resident-facing", "Social care", None, "No"),
            ("Yes, other social care, non-resident-facing", "Social care", "No", None),
            ("Yes, other social care, non-resident-facing", "Social care", None, None),
            
        ],
        schema="work_socialcare string , work_sector string, work_care_nursing_home string,	work_direct_contact string",
    )
    output_df = assign_work_social_column(
        expected_df.drop("work_socialcare"),
        "work_socialcare",
        "work_sector",
        "work_care_nursing_home",
        "work_direct_contact",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_nullable=True)
