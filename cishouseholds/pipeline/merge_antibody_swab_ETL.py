from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_config
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_swabs
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("merge_antibody_swab_ETL")
def merge_antibody_swab_ETL():
    """
    High level function call for running merging process for antibody and swab
    """
    storage_config = get_config()["storage"]
    merge_antibody_ETL(storage_config)
    merge_swab_ETL(storage_config)


def merge_antibody_ETL(storage_config):
    """
    Process for matching and merging survey & swab data
    """
    survey_table = f"{storage_config['table_prefix']}processed_survey_responses_v2"
    antibody_table = f"{storage_config['table_prefix']}processed_blood_test_results"
    survey_df = extract_from_table(survey_table)
    antibody_df = extract_from_table(antibody_table)
    df_best_match, df_not_best_match = execute_and_resolve_flags_merge_specific_antibody(
        survey_df, antibody_df, "date_visit"
    )
    output_df_list = ["df_best_match", "df_not_best_match"]
    output_table_list = ["processed_survey_antibody_merge", "processed_survey_antibody_merge_unmatched"]
    for name, table_name in zip(output_df_list, output_table_list):
        survey_df = update_table(name, table_name)


def merge_swab_ETL(storage_config):
    """
    Process for matching and merging survey & swab data (after merging with antibody)
    """
    survey_table = f"{storage_config['table_prefix']}processed_survey_antibody_merge"
    swab_table = f"{storage_config['table_prefix']}processed_swab_test_results"
    survey_df = extract_from_table(survey_table)
    swab_df = extract_from_table(swab_table)
    df_best_match, df_not_best_match, df_failed_match = execute_and_resolve_flags_merge_specific_swabs(
        survey_df, swab_df, "date_visit"
    )
    output_df_list = ["df_best_match", "df_not_best_match", "df_failed_match"]
    output_table_list = [
        "processed_survey_antibody_swab_merge",
        "processed_survey_antibody_swab_merge_unmatched" "processed_survey_antibody_swab_merge_failed",
    ]
    for name, table_name in zip(output_df_list, output_table_list):
        survey_df = update_table(name, table_name)
