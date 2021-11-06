import pyspark.sql.functions as F

from cishouseholds.merge import join_assayed_bloods
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files():
    """
    Union survey response for v0, v1 and v2, and write to table.
    """

    survey_df_list = []

    for version in ["v0", "v1", "v2"]:
        survey_table = f"transformed_survey_responses_{version}_data"
        survey_df_list.append(extract_from_table(survey_table))

    unioned_survey_responses = union_multiple_tables(survey_df_list)

    update_table(unioned_survey_responses, "unioned_survey_responses", mode_overide="overwrite")


@register_pipeline_stage("outer_join_blood_results")
def outer_join_blood_results():
    """
    Outer join of data for two blood test targets.
    """

    blood_table = "transformed_blood_test_data"
    blood_df = extract_from_table(blood_table)

    blood_df, failed_blood_join_df = join_assayed_bloods(blood_df, test_target_column="antibody_test_target")
    blood_df = blood_df.withColumn(
        "combined_blood_sample_received_date",
        F.coalesce(F.col("blood_sample_received_date_s_protein"), F.col("blood_sample_received_date_n_protein")),
    )

    update_table(blood_df, "joined_blood_test_data", mode_overide="overwrite")
    update_table(failed_blood_join_df, "failed_blood_test_join", mode_overide="overwrite")


@register_pipeline_stage("merge_blood_ETL")
def merge_blood_ETL():
    """
    High level function call for running merging process for blood sample data.
    """
    survey_table = "unioned_survey_responses"
    antibody_table = "joined_blood_test_data"
    survey_df = extract_from_table(survey_table)
    antibody_df = extract_from_table(antibody_table)

    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_blood(survey_df, antibody_df)
    output_antibody_df_list = [survey_antibody_df, antibody_residuals, survey_antibody_failed]
    output_antibody_table_list = [
        "merged_responses_antibody_data",
        "antibody_merge_residuals",
        "antibody_merge_failed_records",
    ]
    load_to_data_warehouse_tables(output_antibody_df_list, output_antibody_table_list)

    return survey_antibody_df


@register_pipeline_stage("merge_swab_ETL")
def merge_swab_ETL():
    """
    High level function call for running merging process for swab sample data.
    """
    survey_table = "merged_responses_antibody_data"
    swab_table = "transformed_swab_test_data"
    survey_df = extract_from_table(survey_table)
    swab_df = extract_from_table(swab_table)

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_swab(survey_df, swab_df)
    output_swab_df_list = [survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed]
    output_swab_table_list = [
        "merged_responses_antibody_swab_data",
        "swab_merge_residuals",
        "swab_merge_failed_records",
    ]
    load_to_data_warehouse_tables(output_swab_df_list, output_swab_table_list)

    return survey_antibody_swab_df


def load_to_data_warehouse_tables(output_df_list, output_table_list):
    for df, table_name in zip(output_df_list, output_table_list):
        update_table(df, table_name, mode_overide="overwrite")


def merge_blood(survey_df, antibody_df):
    """
    Process for matching and merging survey and blood test result data
    """

    survey_antibody_df = execute_merge_specific_antibody(
        survey_df=survey_df,
        labs_df=antibody_df,
        barcode_column_name="blood_sample_barcode",
        visit_date_column_name="visit_date_string",
        received_date_column_name="blood_sample_received_date_s_protein",
    )

    return merge_process_filtering(
        df=survey_antibody_df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
        lab_columns_list=[column for column in antibody_df.columns if column != "blood_sample_barcode"],
    )


def merge_swab(survey_df, swab_df):
    """
    Process for matching and merging survey and swab result data.
    Should be executed after merge with blood test result data.
    """
    survey_antibody_swab_df = execute_merge_specific_swabs(
        survey_df=survey_df,
        labs_df=swab_df,
        barcode_column_name="swab_sample_barcode",
        visit_date_column_name="visit_datetime",
        received_date_column_name="pcr_datetime",
        void_value="void",
    )

    return merge_process_filtering(
        df=survey_antibody_swab_df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
        lab_columns_list=[column for column in swab_df.columns if column != "swab_sample_barcode"],
    )
