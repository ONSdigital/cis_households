# comment
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("merge_blood_ETL")
def merge_blood_ETL():
    """
    High level function call for running merging process for blood sample data.
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]

    survey_table = f"{storage_config['table_prefix']}transformed_survey_responses_v2_data"
    antibody_table = f"{storage_config['table_prefix']}transformed_blood_test_data"
    survey_df = extract_from_table(survey_table, spark_session)
    antibody_df = extract_from_table(antibody_table, spark_session)

    # Merge on antibody test results
    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_blood(survey_df, antibody_df)
    output_antibody_df_list = [survey_antibody_df, antibody_residuals, survey_antibody_failed]
    output_antibody_table_list = [
        "transformed_survey_antibody_merge_data",
        "transformed_antibody_merge_residuals",
        "transformed_survey_antibody_merge_failed",
    ]
    load_to_data_warehouse_tables(output_antibody_df_list, output_antibody_table_list)

    return survey_antibody_df


@register_pipeline_stage("merge_swab_ETL")
def merge_swab_ETL():
    """
    High level function call for running merging process for swab sample data.
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]

    survey_table = f"{storage_config['table_prefix']}transformed_survey_antibody_merge_data"
    swab_table = f"{storage_config['table_prefix']}transformed_swab_test_data"
    survey_df = extract_from_table(survey_table, spark_session)
    swab_df = extract_from_table(swab_table, spark_session)

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_swab(survey_df, swab_df)
    output_swab_df_list = [survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed]
    output_swab_table_list = [
        "transformed_survey_antibody_swab_merge_data",
        "transformed_antibody_swab_merge_residuals",
        "transformed_survey_antibody_swab_merge_failed",
    ]
    load_to_data_warehouse_tables(output_swab_df_list, output_swab_table_list)

    return survey_antibody_swab_df


def load_to_data_warehouse_tables(output_df_list, output_table_list):
    for df, table_name in zip(output_df_list, output_table_list):
        update_table(df, table_name)


def merge_blood(survey_df, antibody_df):
    """
    Process for matching and merging survey and blood test result data
    """

    survey_antibody_df = execute_merge_specific_antibody(
        survey_df=survey_df,
        labs_df=antibody_df,
        barcode_column_name="blood_sample_barcode",
        visit_date_column_name="visit_date_string",
        received_date_column_name="blood_sample_received_date",
    )

    merge_combination_list = ["1tom", "mto1", "mtom"]
    drop_list_columns_antibody = ["drop_flag_mtom_antibody"]  # need to know what to put in this list

    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_process_filtering(
        df=survey_antibody_df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
        lab_columns_list=[column for column in antibody_df.columns if column != "blood_sample_barcode"],
        merge_combination=merge_combination_list,
        drop_list_columns=drop_list_columns_antibody,
    )

    return survey_antibody_df, antibody_residuals, survey_antibody_failed


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

    merge_combination_list = ["1tom", "mto1", "mtom"]
    drop_list_columns_swab = ["drop_flag_mtom_swab"]  # need to know what to put in this list

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_process_filtering(
        df=survey_antibody_swab_df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
        lab_columns_list=[column for column in swab_df.columns if column != "swab_sample_barcode"],
        merge_combination=merge_combination_list,
        drop_list_columns=drop_list_columns_swab,
    )

    return survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed
