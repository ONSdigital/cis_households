from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import transform_cis_soc_data


def test_transform_cis_soc_data(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 1", "JOB1", 5),
            ("JOB TITLE 1", "JOB1", 5),
            ("JOB TITLE 2", "JOB2", 6),
            ("JOB TITLE 2", "JOB2", 66),
            ("JOB TITLE 2", "JOB2", 77),
            ("JOB TITLE 2", "JOB2", "un"),
            ("JOB TITLE 3", "JOB3", "un"),
            (None, "JOB3", "un"),
            ("JOB TITLE 4", "JOB4", ""),
            ("JOB TITLE 5", "JOB5", ""),
            ("JOB TITLE 5", "JOB5", 7),
            ("JOB TITLE 5", "JOB5", 100),
            (None, None, None),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 1", "JOB1", 5, False),
            ("JOB TITLE 3", "JOB3", "uncodeable", True),
            ("JOB TITLE 4", "JOB4", "uncodeable", True),
            ("JOB TITLE 5", "JOB5", 100, False),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string, soc_code_edited_to_uncodeable boolean",
    )
    expected_duplicate_df = spark_session.createDataFrame(
        data=[
            ("JOB TITLE 2", "JOB2", 66, "AMBIGUOUS AFTER DEDUPLICATION", False),
            ("JOB TITLE 2", "JOB2", 77, "AMBIGUOUS AFTER DEDUPLICATION", False),
            ("JOB TITLE 2", "JOB2", 6, "NOT MOST SPECIFIC", False),
            ("JOB TITLE 2", "JOB2", "uncodeable", "UNCODEABLE", True),
            ("JOB TITLE 5", "JOB5", 7, "NOT MOST SPECIFIC", False),
            ("JOB TITLE 5", "JOB5", "uncodeable", "UNCODEABLE", True),
        ],
        schema="work_main_job_title string, work_main_job_role string, standard_occupational_classification_code string, DROP_REASON string, soc_code_edited_to_uncodeable boolean",
    )
    # test mapping functionality with complete map off
    duplicate_df, output_df = transform_cis_soc_data(input_df, ["work_main_job_role"])

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(
        duplicate_df, expected_duplicate_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True
    )
