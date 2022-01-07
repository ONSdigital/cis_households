import pyspark.sql.functions as F

from cishouseholds.merge import join_assayed_bloods
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_transformations


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files(**kwargs):
    """
    Union survey response for v0, v1 and v2, and write to table.
    """
    survey_df_list = []

    for version in ["0", "1", "2"]:
        survey_table = kwargs["transformed_survey_table"].replace("*", version)
        survey_df_list.append(extract_from_table(survey_table))

    unioned_survey_responses = union_multiple_tables(survey_df_list)
    unioned_survey_responses = unioned_survey_responses.dropDuplicates(
        subset=[column for column in unioned_survey_responses.columns if column != "survey_response_source_file"]
    )
    unioned_survey_responses = union_dependent_transformations(unioned_survey_responses)
    update_table(unioned_survey_responses, kwargs["unioned_survey_table"], mode_overide="overwrite")


@register_pipeline_stage("outer_join_blood_results")
def outer_join_blood_results(**kwargs):
    """
    Outer join of data for two blood test targets.
    """
    blood_df = extract_from_table(kwargs["blood_table"])
    blood_df = blood_df.dropDuplicates(
        subset=[column for column in blood_df.columns if column != "blood_test_source_file"]
    )

    blood_df, failed_blood_join_df = join_assayed_bloods(
        blood_df,
        test_target_column="antibody_test_target",
        join_on_columns=[
            "unique_antibody_test_id",
            "blood_sample_barcode",
            "antibody_test_plate_common_id",
            "antibody_test_well_id",
        ],
    )
    blood_df = blood_df.withColumn(
        "combined_blood_sample_received_date",
        F.coalesce(F.col("blood_sample_received_date_s_protein"), F.col("blood_sample_received_date_n_protein")),
    )

    update_table(blood_df, kwargs["antibody_table"], mode_overide="overwrite")
    update_table(failed_blood_join_df, kwargs["failed_blood_table"], mode_overide="overwrite")


@register_pipeline_stage("merge_blood_ETL")
def merge_blood_ETL(**kwargs):
    """
    High level function call for running merging process for blood sample data.
    """
    survey_table = kwargs["unioned_survey_table"]
    antibody_table = kwargs["antibody_table"]

    survey_df = extract_from_table(survey_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    antibody_df = extract_from_table(antibody_table).where(
        F.col("unique_antibody_test_id").isNotNull() & F.col("blood_sample_barcode").isNotNull()
    )

    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_blood(survey_df, antibody_df)

    output_antibody_df_list = [survey_antibody_df, antibody_residuals, survey_antibody_failed]
    output_antibody_table_list = kwargs["antibody_output_tables"]

    load_to_data_warehouse_tables(output_antibody_df_list, output_antibody_table_list)

    return survey_antibody_df


@register_pipeline_stage("merge_swab_ETL")
def merge_swab_ETL(**kwargs):
    """
    High level function call for running merging process for swab sample data.
    """
    survey_table = kwargs["merged_survey_table"]
    swab_table = kwargs["swab_table"]

    survey_df = extract_from_table(survey_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    swab_df = extract_from_table(swab_table).where(
        F.col("unique_pcr_test_id").isNotNull() & F.col("swab_sample_barcode").isNotNull()
    )
    swab_df = swab_df.dropDuplicates(subset=[column for column in swab_df.columns if column != "swab_test_source_file"])

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_swab(survey_df, swab_df)
    output_swab_df_list = [survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed]
    output_swab_table_list = kwargs["swab_output_tables"]
    load_to_data_warehouse_tables(output_swab_df_list, output_swab_table_list)

    return survey_antibody_swab_df


def load_to_data_warehouse_tables(output_df_list, output_table_list):
    for df, table_name in zip(output_df_list, output_table_list):
        update_table(df, table_name, mode_overide="overwrite")


def merge_blood(survey_df, antibody_df):
    """
    Process for matching and merging survey and blood test result data
    """

    survey_antibody_df, none_record_df = execute_merge_specific_antibody(
        survey_df=survey_df,
        labs_df=antibody_df,
        barcode_column_name="blood_sample_barcode",
        visit_date_column_name="visit_date_string",
        received_date_column_name="blood_sample_received_date_s_protein",
    )

    survey_antibody_df = survey_antibody_df.drop(
        "abs_offset_diff_vs_visit_hr_antibody",
        "count_barcode_antibody",
        "count_barcode_voyager",
        "diff_vs_visit_hr_antibody",
    )
    df_all_iqvia, df_lab_residuals, df_failed_records = merge_process_filtering(
        df=survey_antibody_df,
        none_record_df=none_record_df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
        lab_columns_list=[column for column in antibody_df.columns if column != "blood_sample_barcode"],
    )
    return df_all_iqvia, df_lab_residuals, df_failed_records


def merge_swab(survey_df, swab_df):
    """
    Process for matching and merging survey and swab result data.
    Should be executed after merge with blood test result data.
    """
    survey_antibody_swab_df, none_record_df = execute_merge_specific_swabs(
        survey_df=survey_df,
        labs_df=swab_df,
        barcode_column_name="swab_sample_barcode",
        visit_date_column_name="visit_datetime",
        received_date_column_name="pcr_result_recorded_datetime",
        void_value="Void",
    )

    survey_antibody_swab_df = survey_antibody_swab_df.drop(
        "abs_offset_diff_vs_visit_hr_swab",
        "count_barcode_swab",
        "count_barcode_voyager",
        "diff_vs_visit_hr_swab",
        "pcr_flag",
        "time_order_flag",
        "time_difference_flag",
    )
    df_all_iqvia, df_lab_residuals, df_failed_records = merge_process_filtering(
        df=survey_antibody_swab_df,
        none_record_df=none_record_df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
        lab_columns_list=[column for column in swab_df.columns if column != "swab_sample_barcode"],
    )
    return df_all_iqvia, df_lab_residuals, df_failed_records
