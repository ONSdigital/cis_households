from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering


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
