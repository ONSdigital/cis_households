from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_config
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("merge_antibody_swab_ETL")
def merge_antibody_swab_ETL():
    """
    High level function call for running merging process for antibody and swab
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]
    # ANTIBODY
    survey_antibody_df, antibody_df = extract_from_data_warehouse(storage_config, spark_session, "antibody")
    survey_antibody_df, survey_antibody_residuals, survey_antibody_failed = transform_antibody_swab_ETL(
        survey_antibody_df, antibody_df, "antibody"
    )

    output_df_anitbody_list = [survey_antibody_df, survey_antibody_residuals, survey_antibody_failed]
    output_table_antibody_list = [
        "transformed_survey_antibody_merge_data",
        "transformed_survey_antibody_merge_residuals",
        "transformed_survey_antibody_merge_failed",
    ]
    survey_antibody_df = load_to_data_warehouse(output_df_anitbody_list, output_table_antibody_list)

    # SWAB
    survey_antibody_swab_df, swab_df = extract_from_data_warehouse(storage_config, spark_session, "swab")
    survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed = transform_antibody_swab_ETL(
        survey_antibody_swab_df, swab_df, "swab"
    )
    output_df_swab_list = [survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed]
    output_table_swab_list = [
        "transformed_survey_antibody_swab_merge",
        "transformed_survey_antibody_swab_merge_residuals",
        "transformed_survey_antibody_swab_merge_failed",
    ]
    survey_antibody_swab_df = load_to_data_warehouse(output_df_swab_list, output_table_swab_list)

    return survey_antibody_swab_df


def extract_from_data_warehouse(storage_config, spark_session, merge_type: str):
    if merge_type == "antibody":
        survey_table = f"{storage_config['table_prefix']}transformed_survey_responses_v2_data"
        labs_table = f"{storage_config['table_prefix']}transformed_blood_test_data"
    else:
        survey_table = f"{storage_config['table_prefix']}transformed_survey_antibody_merge_data"
        labs_table = f"{storage_config['table_prefix']}transformed_swab_test_data"

    survey_df = extract_from_table(survey_table, spark_session)
    labs_df = extract_from_table(labs_table, spark_session)

    return survey_df, labs_df


def transform_antibody_swab_ETL(survey_df, labs_df, merge_type: str):
    if merge_type == "antibody":
        antibody_df = labs_df
        survey_antibody_df, survey_antibody_residuals, survey_antibody_failed = merge_antibody_ETL(
            survey_df, antibody_df
        )

        return survey_antibody_df, survey_antibody_residuals, survey_antibody_failed
    else:
        swab_df = labs_df
        survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed = merge_swab_ETL(
            survey_df, swab_df
        )

        return survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed


def load_to_data_warehouse(output_df_list, output_table_list):
    for df, table_name in zip(output_df_list, output_table_list):
        table_df = update_table(df, table_name)

    return table_df


def merge_antibody_ETL(survey_df, antibody_df):
    """
    Process for matching and merging survey & swab data
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
        "siemens",
    ]
    merge_combination_list = ["1tom", "mto1", "mtom"]
    drop_list_columns_antibody = ["drop_flag_mtom_antibody"]  # need to know what to put in this list

    survey_antibody_df, survey_antibody_residuals, survey_antibody_failed = merge_process_filtering(
        survey_antibody_df,
        "antibody",
        "blood_sample_barcode",
        antibody_columns_list,
        merge_combination_list,
        drop_list_columns_antibody,
    )

    return survey_antibody_df, survey_antibody_residuals, survey_antibody_failed


def merge_swab_ETL(survey_df, swab_df):
    """
    Process for matching and merging survey & swab data (after merging with antibody)
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

    survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed = merge_process_filtering(
        survey_antibody_swab_df,
        "swab",
        "swab_sample_barcode",
        swab_columns_list,
        merge_combination_list,
        drop_list_columns_swab,
    )

    return survey_antibody_swab_df, survey_antibody_swab_residuals, survey_antibody_swab_failed
