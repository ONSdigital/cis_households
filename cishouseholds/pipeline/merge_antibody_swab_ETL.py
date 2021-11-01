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
        survey_df,
        antibody_df,
        "blood_sample_barcode",
        "visit_date_string",
        "blood_sample_received_date",
    )

    antibody_columns_list = [
        "blood_sample_type",
        "antibody_test_plate_id",
        "antibody_test_well_id",
        "antibody_test_result_classification",
        "antibody_test_result_value",
        "antibody_test_bounded_result_value",
        "antibody_test_undiluted_result_value",
        "antibody_test_result_recorded_date",
        "blood_sample_arrayed_date",
        "blood_sample_received_date",
        "blood_sample_collected_datetime",
        "blood_test_source_file",
        "antibody_test_target",
        "plate",
        "assay_category",
        "assay_siemens",
    ]
    merge_combination_list = ["1tom", "mto1", "mtom"]
    drop_list_columns_antibody = ["drop_flag_mtom_antibody"]  # need to know what to put in this list

    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_process_filtering(
        survey_antibody_df,
        "antibody",
        "blood_sample_barcode",
        antibody_columns_list,
        merge_combination_list,
        drop_list_columns_antibody,
    )

    return survey_antibody_df, antibody_residuals, survey_antibody_failed


def merge_swab(survey_df, swab_df):
    """
    Process for matching and merging survey and swab result data.
    Should be executed after merge with blood test result data.
    """
    survey_antibody_swab_df = execute_merge_specific_swabs(
        survey_df,
        swab_df,
        "swab_sample_barcode",
        "visit_datetime",
        "pcr_datetime",
        "void",
    )

    swab_columns_list = [
        "pcr_result_classification",
        "pcr_datetime",
        "pcr_lab_id",
        "pcr_method",
        "orf1ab_gene_pcr_target",
        "orf1ab_gene_pcr_result_classification",
        "orf1ab_gene_pcr_cq_value",
        "n_gene_pcr_target",
        "n_gene_pcr_result_classification",
        "n_gene_pcr_cq_value",
        "s_gene_pcr_target",
        "s_gene_pcr_result_classification",
        "s_gene_pcr_cq_value",
        "ms2_pcr_target",
        "ms2_pcr_result_classification",
        "ms2_pcr_cq_value",
        "swab_test_source_file",
        "pcr_date",
        "cq_pattern",
        "mean_pcr_cq_value",
        "one_positive_pcr_target_only",
    ]
    merge_combination_list = ["1tom", "mto1", "mtom"]
    drop_list_columns_swab = ["drop_flag_mtom_swab"]  # need to know what to put in this list

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_process_filtering(
        survey_antibody_swab_df,
        "swab",
        "swab_sample_barcode",
        swab_columns_list,
        merge_combination_list,
        drop_list_columns_swab,
    )

    return survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed
