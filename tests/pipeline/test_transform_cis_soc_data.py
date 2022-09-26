from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import transform_cis_soc_data


def test_transform_cis_soc_data(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 1", "JOB1", 5, "A"),
            ("JOB TITLE 1", "JOB1", 5, "B"),  # Complete duplicates get deduplicated
            ("JOB TITLE 2", "JOB2", 6, "C"),  # Less specific, so this is dropped
            ("JOB TITLE 2", "JOB2", 66, "D"),
            ("JOB TITLE 2", "JOB2", 77, "E"),  # Two codes with equally high specificity, so require manual resolution
            ("JOB TITLE 2", "JOB2", "un", "F"),  # Duplicates have codes, so this is dropped
            ("JOB TITLE 3", "JOB3", "un", "G"),
            (None, "JOB3", "un", "H"),
            ("JOB TITLE 4", "JOB4", "", "I"),
            ("JOB TITLE 5", "JOB5", "", "J"),  # Duplicates have codes, so this is dropped
            ("JOB TITLE 5", "JOB5", 7, "K"),
            ("JOB TITLE 5", "JOB5", 100, "L"),  # One code with higher specificity, so can be automatically resolved
            (None, None, None, "M"),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string, another_column string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 1", "JOB1", 5, "A"),
            ("JOB TITLE 3", "JOB3", "uncodeable", "G"),
            ("JOB TITLE 4", "JOB4", "uncodeable", "I"),
            ("JOB TITLE 5", "JOB5", 100, "L"),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string, another_column string",
    )
    expected_conflicts_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 2", "JOB2", 66, "D"),
            ("JOB TITLE 2", "JOB2", 77, "E"),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string, another_column string",
    )
    # test mapping functionality with complete map off
    duplicate_df, output_df = transform_cis_soc_data(input_df, ["work_main_job_role"])

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(
        duplicate_df, expected_conflicts_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True
    )
