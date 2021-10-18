from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_and_resolve_flags_merge_specific_swabs
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("merge_antibody_etl")
def merge_antibody_etl():
    """
    Process for matching and merging survey & swab data
    """
    survey_table = "processed_survey_responses_v2"
    antibody_table = "processed_blood_test_results"
    survey_df = extract_from_table(survey_table)
    antibody_df = extract_from_table(antibody_table)
    df_best_match, df_not_best_match = execute_and_resolve_flags_merge_specific_antibody(
        survey_df, antibody_df, "date_visit"
    )
    output_df_list = ["df_best_match", "df_not_best_match"]
    output_table_list = ["processed_survey_antibody_merge", "processed_survey_antibody_merge_unmatched"]
    for name, table_name in zip(output_df_list, output_table_list):
        survey_df = update_table(name, table_name)


@register_pipeline_stage("merge_swab_etl")
def merge_swab_etl():
    """
    Process for matching and merging survey & swab data
    """
    survey_table = "processed_survey_responses_v2"
    swab_table = "processed_swab_test_results"
    survey_df = extract_from_table(survey_table)
    swab_df = extract_from_table(swab_table)
    df_best_match, df_not_best_match, df_failed_match = execute_and_resolve_flags_merge_specific_swabs(
        survey_df, swab_df, "date_visit"
    )
    output_df_list = ["df_best_match", "df_not_best_match", "df_failed_match"]
    output_table_list = [
        "processed_survey_antibody_merge",
        "processed_survey_antibody_merge_unmatched" "processed_survey_antibody_merge_failed",
    ]
    for name, table_name in zip(output_df_list, output_table_list):
        survey_df = update_table(name, table_name)
