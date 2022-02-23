import pyspark.sql.functions as F

from cishouseholds.filter import file_exclude
from cishouseholds.merge import join_assayed_bloods
from cishouseholds.merge import union_dataframes_to_hive
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.merge_process import execute_merge_specific_antibody
from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs
from cishouseholds.pipeline.merge_process import merge_process_filtering
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_cleaning
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_derivations
from cishouseholds.pipeline.validation_ETL import validation_ETL
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files(transformed_survey_responses_table_pattern: str, unioned_survey_responses_table: str):
    """
    Union survey response for v0, v1 and v2, and write to table.
    """
    survey_df_list = []

    for version in ["0", "1", "2"]:
        survey_table = transformed_survey_responses_table_pattern.replace("*", version)
        survey_df_list.append(extract_from_table(survey_table))

    union_dataframes_to_hive(unioned_survey_responses_table, survey_df_list)


@register_pipeline_stage("union_dependent_transformations")
def execute_union_dependent_transformations(unioned_survey_table: str, transformed_table: str):
    """
    Transformations that require the union of the different input survey response files.
    Includes combining data from different files and filling forwards or backwards over time.
    """
    unioned_survey_responses = extract_from_table(unioned_survey_table)
    unioned_survey_responses = union_dependent_cleaning(unioned_survey_responses)
    unioned_survey_responses = union_dependent_derivations(unioned_survey_responses)
    update_table(unioned_survey_responses, transformed_table, mode_overide="overwrite")


@register_pipeline_stage("validate_survey_responses")
def validate_survey_responses(
    survey_responses_table: str,
    duplicate_count_column_name: str,
    validation_failure_flag_column: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
):
    unioned_survey_responses = extract_from_table(survey_responses_table)
    valid_survey_responses, erroneous_survey_responses = validation_ETL(
        df=unioned_survey_responses,
        validation_check_failure_column_name=validation_failure_flag_column,
        duplicate_count_column_name=duplicate_count_column_name,
    )
    update_table(valid_survey_responses, valid_survey_responses_table, mode_overide="overwrite")
    update_table(erroneous_survey_responses, invalid_survey_responses_table, mode_overide="overwrite")


@register_pipeline_stage("lookup_based_editing")
def lookup_based_editing(
    input_table: str, cohort_lookup_path: str, travel_countries_lookup_path: str, edited_table: str
):
    """Edit columns based on mappings from lookup files. Often used to correct data quality issues."""
    df = extract_from_table(input_table)

    spark = get_or_create_spark_session()
    cohort_lookup = spark.read.csv(
        cohort_lookup_path, header=True, schema="participant_id string, new_cohort string, old_cohort string"
    ).withColumnRenamed("participant_id", "cohort_participant_id")
    travel_countries_lookup = spark.read.csv(
        travel_countries_lookup_path,
        header=True,
        schema="been_outside_uk_last_country_old string, been_outside_uk_last_country_new string",
    )

    df = df.join(
        cohort_lookup,
        how="left",
        on=((df.participant_id == cohort_lookup.cohort_participant_id) & (df.study_cohort == cohort_lookup.old_cohort)),
    )
    df = df.withColumn("study_cohort", F.coalesce(F.col("new_cohort"), F.col("study_cohort"))).drop(
        "new_cohort", "old_cohort"
    )

    df = df.join(
        travel_countries_lookup,
        how="left",
        on=df.been_outside_uk_last_country == travel_countries_lookup.been_outside_uk_last_country_old,
    )
    df = df.withColumn(
        "been_outside_uk_last_country",
        F.coalesce(F.col("been_outside_uk_last_country_new"), F.col("been_outside_uk_last_country")),
    ).drop("been_outside_uk_last_country_old", "been_outside_uk_last_country_new")

    update_table(df, edited_table, mode_overide="overwrite")


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
    survey_file_exclude_list = kwargs["files_to_exclude_survey"]
    blood_file_exclude_list = kwargs["files_to_exclude_blood"]

    survey_df = extract_from_table(survey_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    survey_df = file_exclude(survey_df, "survey_response_source_file", survey_file_exclude_list)

    antibody_df = extract_from_table(antibody_table).where(
        F.col("unique_antibody_test_id").isNotNull() & F.col("blood_sample_barcode").isNotNull()
    )
    antibody_df = file_exclude(antibody_df, "blood_test_source_file", blood_file_exclude_list)

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
    survey_file_exclude_list = kwargs["files_to_exclude_survey"]
    swab_file_exclude_list = kwargs["files_to_exclude_swab"]

    survey_df = extract_from_table(survey_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    survey_df = file_exclude(survey_df, "survey_response_source_file", survey_file_exclude_list)

    swab_df = extract_from_table(swab_table).where(
        F.col("unique_pcr_test_id").isNotNull() & F.col("swab_sample_barcode").isNotNull()
    )
    swab_df = file_exclude(swab_df, "swab_test_source_file", swab_file_exclude_list)

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
