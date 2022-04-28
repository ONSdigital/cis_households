from typing import List

from cishouseholds.filter import file_exclude
from cishouseholds.merge import many_to_many_flag
from cishouseholds.merge import many_to_one_antibody_flag
from cishouseholds.merge import many_to_one_swab_flag
from cishouseholds.merge import one_to_many_antibody_flag
from cishouseholds.merge import one_to_many_swabs
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.merge_process import merge_process_preparation


def load_to_data_warehouse_tables(output_df_list, output_table_list):
    for df, table_name in zip(output_df_list, output_table_list):
        update_table(df, table_name, mode_overide="overwrite")


# merge_blood no longer used in the main ETL mege function
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


# merge_swab no longer used in the main ETL mege function
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


# merge substages ANTIBODY ~~~~~~~~~~~~~~~~~
def merge_blood_process_preparation(
    survey_df,
    antibody_df,
    blood_files_to_exclude: List[str],
):
    antibody_df = file_exclude(antibody_df, "blood_test_source_file", blood_files_to_exclude)

    merged_1to1, df = merge_process_preparation(
        survey_df=survey_df,
        labs_df=antibody_df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
        visit_date_column_name="visit_datetime",
        received_date_column_name="blood_sample_received_date_s_protein",
    )
    return merged_1to1, df


def merge_blood_xtox_flag(df):
    """ """
    merge_type = "antibody"

    df = one_to_many_antibody_flag(
        df=df,
        column_name_to_assign="drop_flag_1tom_" + merge_type,
        group_by_column="blood_sample_barcode",
        diff_interval_hours="diff_vs_visit_hr_antibody",
        siemens_column="siemens_antibody_test_result_value_s_protein",
        tdi_column="antibody_test_result_classification_s_protein",
        visit_date="visit_datetime",
    )
    df = many_to_one_antibody_flag(
        df=df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column="blood_sample_barcode",
    )
    window_columns = [
        "abs_offset_diff_vs_visit_hr_antibody",
        "diff_vs_visit_hr_antibody",
        "unique_participant_response_id",
        "unique_antibody_test_id",
    ]
    df = many_to_many_flag(
        df=df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column="blood_sample_barcode",
        ordering_columns=window_columns,
        process_type=merge_type,
        out_of_date_range_column="out_of_date_range_" + merge_type,
        failure_column_name="failed_flag_mtom_" + merge_type,
    )
    return df


def merge_blood_process_filtering(df):
    """ """
    df_all_iqvia, df_lab_residuals, df_failed_records = merge_process_filtering(
        df=df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
        lab_columns_list=[column for column in df.columns if column != "blood_sample_barcode"],
    )
    output_antibody_df_list = [
        df_all_iqvia,  # survey_antibody_swab_df,
        df_lab_residuals,  # antibody_swab_residuals,
        df_failed_records,  # survey_antibody_swab_failed
    ]
    return output_antibody_df_list


# merge substages SWAB ~~~~~~~~~~~~~~~~~
def merge_swab_process_preparation(
    survey_df,
    swab_df,
    swab_files_to_exclude: List[str],
):
    swab_df = file_exclude(swab_df, "swab_test_source_file", swab_files_to_exclude)
    swab_df = swab_df.dropDuplicates(subset=[column for column in swab_df.columns if column != "swab_test_source_file"])
    merged_1to1, df = merge_process_preparation(
        survey_df=survey_df,
        labs_df=swab_df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
        visit_date_column_name="visit_datetime",
        received_date_column_name="pcr_result_recorded_datetime",
    )
    return merged_1to1, df


def merge_swab_xtox_flag(df):
    merge_type = "swab"
    window_columns = [
        "abs_offset_diff_vs_visit_hr_swab",
        "diff_vs_visit_hr_swab",
        "visit_datetime",
        # Stata also uses uncleaned barcode from labs
    ]
    df = one_to_many_swabs(
        df=df,
        group_by_column="swab_sample_barcode",
        ordering_columns=window_columns,
        pcr_result_column_name="pcr_result_classification",
        void_value="Void",
        flag_column_name="drop_flag_1tom_" + merge_type,
    )
    df = many_to_one_swab_flag(
        df=df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column="swab_sample_barcode",
        ordering_columns=window_columns,
    )
    df = many_to_many_flag(
        df=df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column="swab_sample_barcode",
        out_of_date_range_column="out_of_date_range_swab",
        ordering_columns=[
            "abs_offset_diff_vs_visit_hr_swab",
            "diff_vs_visit_hr_swab",
            "unique_participant_response_id",
            "unique_pcr_test_id",
        ],
        process_type=merge_type,
        failure_column_name="failed_flag_mtom_" + merge_type,
    )
    return df


def merge_swab_process_filtering(df):
    df_all_iqvia, df_lab_residuals, df_failed_records = merge_process_filtering(
        df=df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
        lab_columns_list=[column for column in df.columns if column != "swab_sample_barcode"],
    )
    output_swab_df_list = [
        df_all_iqvia,  # survey_antibody_swab_df,
        df_lab_residuals,  # antibody_swab_residuals,
        df_failed_records,  # survey_antibody_swab_failed
    ]
    return output_swab_df_list
