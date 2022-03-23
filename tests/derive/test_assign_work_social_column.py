from chispa import assert_df_equality

from cishouseholds.derive import assign_work_social_column


def test_assign_work_social_colum(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (None, None, "No", "Yes"),
            (None, None, None, None),
            ("No", "Self-employed", "No", "Yes"),
            ("Yes, care/residential home, resident-facing", "Furloughed (temporarily not working)", "Yes", "Yes"),
            (None, "Furloughed (temporarily not working)", None, None),
            ("Yes, other social care, resident-facing", "Furloughed (temporarily not working)", "No", "Yes"),
            ("Yes, care/residential home, non-resident-facing", "Social Care", "Yes", None),
            ("Yes, other social care, non-resident-facing", "Social Care", None, "No"),
            ("Yes, care/residential home, non-resident-facing", "Social Care", "Yes", "No"),
            ("Yes, care/residential home, non-resident-facing", "Social Care", "Yes", None),
            ("Yes, other social care, non-resident-facing", "Social Care", None, "No"),  # None
            ("Yes, care/residential home, non-resident-facing", "Social Care", "Yes", None),  # No
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
