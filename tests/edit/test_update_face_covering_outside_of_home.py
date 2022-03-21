from chispa import assert_df_equality

from cishouseholds.edit import update_face_covering_outside_of_home


def test_update_face_covering_outside_of_home(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("Never", "Never", "Original"),
            ("Yes, sometimes", "Yes, always", "Original"),
            ("My face is already covered", "Never", "Original"),
            ("Yes, sometimes", "Never", "Original"),
            ("Yes, sometimes", None, None),
            (None, "Yes, sometimes", None),
            ("My face is already covered", "Yes, sometimes", None),
            ("Yes, sometimes", "My face is already covered", None),
            ("My face is already covered", None, None),
            (None, "My face is already covered", None),
            ("My face is already covered", "My face is already covered", None),
            (None, None, "My face is already covered"),
            (None, None, None),
        ],
        schema="""enclosed string, work string, out string""",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("Never", "Never", "No"),
            ("Yes, sometimes", "Yes, always", "Yes, usually both Work/school/other"),
            ("My face is already covered", "Never", "My face is already covered"),
            ("Yes, sometimes", "Never", "Yes, in other situations only"),
            ("Yes, sometimes", None, "Yes, in other situations only"),
            (None, "Yes, sometimes", "Yes, at work/school only"),
            ("My face is already covered", "Yes, sometimes", "Yes, usually both Work/school/other"),
            ("Yes, sometimes", "My face is already covered", "Yes, usually both Work/school/other"),
            ("My face is already covered", None, "My face is already covered"),
            (None, "My face is already covered", "My face is already covered"),
            ("My face is already covered", "My face is already covered", "My face is already covered"),
            (None, None, "My face is already covered"),
            (None, None, None),
        ],
        schema="""enclosed string, work string, out string""",
    )
    output_df = update_face_covering_outside_of_home(input_df, "out", "enclosed", "work")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
