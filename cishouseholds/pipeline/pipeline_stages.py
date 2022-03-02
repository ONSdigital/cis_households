from datetime import datetime
from datetime import timedelta
from functools import reduce
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import List
from typing import Union

import pandas as pd
from pyspark.sql import functions as F

from cishouseholds.derive import assign_multigeneration
from cishouseholds.edit import update_from_csv_lookup
from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.filter import file_exclude
from cishouseholds.hdfs_utils import read_header
from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.merge import join_assayed_bloods
from cishouseholds.merge import union_dataframes_to_hive
from cishouseholds.pipeline.category_map import category_maps
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.generate_outputs import map_output_values_and_column_names
from cishouseholds.pipeline.generate_outputs import write_csv_rename
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import extract_df_list
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.manifest import Manifest
from cishouseholds.pipeline.merge_antibody_swab_ETL import load_to_data_warehouse_tables
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab
from cishouseholds.pipeline.post_merge_processing import derive_overall_vaccination
from cishouseholds.pipeline.post_merge_processing import filter_response_records
from cishouseholds.pipeline.post_merge_processing import impute_key_columns
from cishouseholds.pipeline.post_merge_processing import merge_dependent_transform
from cishouseholds.pipeline.post_merge_processing import nims_transformations
from cishouseholds.pipeline.survey_responses_version_2_ETL import fill_forwards_transformations
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_cleaning
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_derivations
from cishouseholds.pipeline.validation_ETL import validation_ETL
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_files
from cishouseholds.weights.population_projections import proccess_population_projection_df
from cishouseholds.weights.pre_calibration import pre_calibration_high_level
from cishouseholds.weights.weights import generate_weights
from cishouseholds.weights.weights import household_level_populations
from cishouseholds.weights.weights import prepare_auxillary_data
from dummy_data_generation.generate_data import generate_historic_bloods_data
from dummy_data_generation.generate_data import generate_nims_table
from dummy_data_generation.generate_data import generate_ons_gl_report_data
from dummy_data_generation.generate_data import generate_survey_v0_data
from dummy_data_generation.generate_data import generate_survey_v1_data
from dummy_data_generation.generate_data import generate_survey_v2_data
from dummy_data_generation.generate_data import generate_unioxf_medtest_data


pipeline_stages = {}


def register_pipeline_stage(key):
    """Decorator to register a pipeline stage function."""

    def _add_pipeline_stage(func):
        pipeline_stages[key] = func
        return func

    return _add_pipeline_stage


@register_pipeline_stage("delete_tables")
def delete_tables(prefix: str = None, table_names: Union[str, List[str]] = None, pattern: str = None):
    """
    Deletes HIVE tables. For use at the start of a pipeline run, to reset pipeline logs and data.
    Should not be used in production, as all tables may be deleted.

    Use one or more of the optional parameters.

    Parameters
    ----------
    prefix
        remove all tables with a given table name prefix (see config for current prefix)
    table_names
        one or more absolute table names to delete (including prefix)
    pattern
        drop tables where table name matches pattern in SQL format (e.g. "%_responses_%")
    """
    spark_session = get_or_create_spark_session()
    storage_config = get_config()["storage"]

    if table_names is not None:
        if type(table_names) != list:
            table_names = [table_names]  # type:ignore
        for table_name in table_names:
            print(
                f"dropping table: {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
            )  # functional
            spark_session.sql(
                f"DROP TABLE IF EXISTS {storage_config['database']}.{storage_config['table_prefix']}{table_name}"
            )
    if pattern is not None:
        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '{pattern}'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        for table_name in tables:
            print(f"dropping table: {table_name}")  # functional
            spark_session.sql(f"DROP TABLE IF EXISTS {storage_config['database']}.{table_name}")
    if prefix is not None:
        tables = (
            spark_session.sql(f"SHOW TABLES IN {storage_config['database']} LIKE '{prefix}*'")
            .select("tableName")
            .toPandas()["tableName"]
            .tolist()
        )
        for table_name in tables:
            print(f"dropping table: {table_name}")  # functional
            spark_session.sql(f"DROP TABLE IF EXISTS {storage_config['database']}.{table_name}")


@register_pipeline_stage("generate_dummy_data")
def generate_dummy_data(output_directory):
    raw_dir = Path(output_directory) / "generated_data"
    swab_dir = raw_dir / "swab"
    blood_dir = raw_dir / "blood"
    survey_dir = raw_dir / "survey"
    northern_ireland_dir = raw_dir / "northern_ireland_sample"
    sample_direct_dir = raw_dir / "england_wales_sample"
    unprocessed_bloods_dir = raw_dir / "unprocessed_blood"
    historic_bloods_dir = raw_dir / "historic_blood"
    historic_swabs_dir = raw_dir / "historic_swab"
    historic_survey_dir = raw_dir / "historic_survey"
    for directory in [
        swab_dir,
        blood_dir,
        survey_dir,
        northern_ireland_dir,
        sample_direct_dir,
        unprocessed_bloods_dir,
        historic_bloods_dir,
        historic_swabs_dir,
        historic_survey_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)

    file_datetime = datetime.now()
    lab_date_1 = datetime.strftime(file_datetime - timedelta(days=1), format="%Y%m%d")
    lab_date_2 = datetime.strftime(file_datetime - timedelta(days=2), format="%Y%m%d")
    file_date = datetime.strftime(file_datetime, format="%Y%m%d")

    # Historic files
    # historic_bloods = generate_historic_bloods_data(historic_bloods_dir, file_date, 30)
    # historic_swabs = generate_ons_gl_report_data(historic_swabs_dir, file_date, 30)

    # historic_v2 = generate_survey_v2_data(
    #     directory=historic_survey_dir,
    #     file_date=file_date,
    #     records=100,
    #     swab_barcodes=historic_swabs["Sample"].unique().tolist(),
    #     blood_barcodes=historic_bloods["blood_barcode_OX"].unique().tolist(),
    # )

    # Delta files
    lab_swabs_1 = generate_ons_gl_report_data(swab_dir, file_date, 10)
    lab_swabs_2 = generate_ons_gl_report_data(swab_dir, lab_date_1, 10)
    lab_swabs_3 = generate_ons_gl_report_data(swab_dir, lab_date_2, 10)
    lab_swabs = pd.concat([lab_swabs_1, lab_swabs_2, lab_swabs_3])

    lab_bloods_s_1, lab_bloods_n_1 = generate_unioxf_medtest_data(blood_dir, file_date, 10)
    lab_bloods_s_2, lab_bloods_n_2 = generate_unioxf_medtest_data(blood_dir, lab_date_1, 10)
    lab_bloods_s_3, lab_bloods_n_3 = generate_unioxf_medtest_data(blood_dir, lab_date_2, 10)

    lab_bloods = pd.concat(
        [lab_bloods_n_1, lab_bloods_n_2, lab_bloods_n_3, lab_bloods_s_1, lab_bloods_s_2, lab_bloods_s_3]
    )

    historic_blood_n = generate_historic_bloods_data(historic_bloods_dir, file_date, 10, "N")
    historic_blood_s = generate_historic_bloods_data(historic_bloods_dir, file_date, 10, "S")

    # unprocessed_bloods_data = generate_unprocessed_bloods_data(unprocessed_bloods_dir, file_date, 20)
    # northern_ireland_data = generate_northern_ireland_data(northern_ireland_dir, file_date, 20)
    # sample_direct_data = generate_sample_direct_data(sample_direct_dir, file_date, 20)

    # swab/blood barcode lists
    swab_barcode = lab_swabs["Sample"].unique().tolist()
    blood_barcode = lab_bloods["Serum Source ID"].unique().tolist()
    blood_barcode += historic_blood_n["blood_barcode_OX"].unique().tolist()
    blood_barcode += historic_blood_s["blood_barcode_OX"].unique().tolist()

    swab_barcode = swab_barcode[int(round(len(swab_barcode) / 10)) :]  # noqa: E203
    blood_barcode = blood_barcode[int(round(len(swab_barcode) / 10)) :]  # noqa: E203

    generate_survey_v0_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )
    generate_survey_v1_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )
    v2 = generate_survey_v2_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=swab_barcode, blood_barcodes=blood_barcode
    )

    participant_ids = v2["Participant_id"].unique().tolist()

    generate_nims_table(get_full_table_name("cis_nims_20210101"), participant_ids)


def generate_input_processing_function(
    stage_name,
    validation_schema,
    column_name_map,
    datetime_column_map,
    transformation_functions,
    output_table_name,
    source_file_column,
    write_mode="overwrite",
    sep=",",
    cast_to_double_list=[],
    include_hadoop_read_write=True,
):
    """
    Generate an input file processing stage function and register it.

    Returns dataframe for use in testing.

    Parameters
    ----------
    include_hadoop_read_write
        set to False for use in testing on non-hadoop environments

    Notes
    -----
    See underlying functions for other parameter documentation.
    """

    @register_pipeline_stage(stage_name)
    def _inner_function(
        resource_path, latest_only=False, start_date=None, end_date=None, include_processed=False, include_invalid=False
    ):
        file_path_list = [resource_path]
        if include_hadoop_read_write:
            file_path_list = get_files_to_be_processed(
                resource_path,
                latest_only=latest_only,
                start_date=start_date,
                end_date=end_date,
                include_processed=include_processed,
                include_invalid=include_invalid,
            )
        if not file_path_list:
            print(f"        - No files selected in {resource_path}")  # functional
            return

        valid_file_paths = validate_files(file_path_list, validation_schema, sep=sep)
        if not valid_file_paths:
            print(f"        - No valid files found in: {resource_path}.")  # functional
            return

        df = extract_validate_transform_input_data(
            resource_path=file_path_list,
            variable_name_map=column_name_map,
            datetime_map=datetime_column_map,
            validation_schema=validation_schema,
            transformation_functions=transformation_functions,
            sep=sep,
            cast_to_double_columns_list=cast_to_double_list,
        )
        if include_hadoop_read_write:
            update_table_and_log_source_files(df, output_table_name, source_file_column, write_mode)
        return df

    _inner_function.__name__ = stage_name
    return _inner_function


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files(transformed_survey_responses_table_pattern: str, unioned_survey_responses_table: str):
    """
    Union survey response for v0, v1 and v2, and write to table.
    Parameters
    ----------
    transformed_survey_responses_table_pattern
        input table pattern for extracting each of the transformed survey responses tables
    unioned_survey_responses_table
        output table name for the combine file of 3 unioned survey responses
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
    Parameters
    ----------
    unioned_survey_table
        input table name for table containing the combined survey responses tables
    transformed_table
        output table name for table with applied transformations dependent on complete survey dataset
    """
    unioned_survey_responses = extract_from_table(unioned_survey_table)
    unioned_survey_responses = union_dependent_cleaning(unioned_survey_responses)
    unioned_survey_responses = union_dependent_derivations(unioned_survey_responses)
    update_table(unioned_survey_responses, transformed_table, mode_overide="overwrite")


@register_pipeline_stage("fill_forwards_stage")
def fill_forwards_stage(unioned_survey_table: str, filled_forwards_table: str):
    df = extract_from_table(unioned_survey_table)
    df = fill_forwards_transformations(df)
    update_table(df, filled_forwards_table, mode_overide="overwrite")


@register_pipeline_stage("validate_survey_responses")
def validate_survey_responses(
    survey_responses_table: str,
    duplicate_count_column_name: str,
    validation_failure_flag_column: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
):
    """
    Populate error column with outcomes of specific validation checks against fully
    transformed survey dataset.

    Parameters
    ----------
    survey_responses_table
        input table name for fully transformed survey table
    duplicate_count_column_name
        column name in which to count duplicates of rows within the dataframe
    validation_failure_flag_column
        name for error column wherein each of the validation checks results are appended
    valid_survey_responses_table
        table containing results that passed the error checking process
    invalid_survey_responses_table
        table containing results that failed the error checking process
    """
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
    """
    Edit columns based on mappings from lookup files. Often used to correct data quality issues.
    Parameters
    ----------
    input_table
        input table name for reference table
    cohort_lookup_path
        input file path name for cohort corrections lookup file
    travel_countries_lookup_path
        input file path name for travel_countries corrections lookup file
    edited_table
    """
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


@register_pipeline_stage("outer_join_antibody_results")
def outer_join_antibody_results(
    antibody_test_result_table: str, joined_antibody_test_result_table: str, failed_join_table: str
):
    """
    Outer join of data for two antibody/blood test targets (S and N protein antibodies).
    Creates a single record per blood sample.

    Parameters
    ----------
    antibody_test_result_table
        name of HIVE table to read antibody/blood test results from, where blood samples may have more than one record
    joined_antibody_test_result_table
        name of HIVE table to write successfully joined records, where each blood sample has one record
    failed_join_table
        name of HIVE table to write antibody/blood test results that failed to merge.
        Specifically, those with more than two records in the unjoined data.
    """
    blood_df = extract_from_table(antibody_test_result_table)
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

    update_table(blood_df, joined_antibody_test_result_table, mode_overide="overwrite")
    update_table(failed_blood_join_df, failed_join_table, mode_overide="overwrite")


@register_pipeline_stage("merge_blood_ETL")
def merge_blood_ETL(
    survey_responses_table: str,
    antibody_table: str,
    blood_files_to_exclude: List[str],
    antibody_output_tables: List[str],
):
    """
    High level function for joining antibody/blood test result data to survey responses.
    Should be run before the PCR/swab result merge.

    Parameters
    ----------
    survey_responses_table
        name of HIVE table containing survey response records
    antibody_table
        name of HIVE table containing antibody/blood result records
    swab_files_to_exclude
        antibody/blood result files that should be excluded from the merge.
        Used to remove files that are found to contain invalid data.
    swab_output_tables
        names of the three output tables:
            1. survey responses and successfully joined results
            2. residual antibody/blood result records, where there was no barcode match to join on
            3. antibody/blood result records that failed to meet the criteria for joining
    """

    survey_df = extract_from_table(survey_responses_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    antibody_df = extract_from_table(antibody_table).where(
        F.col("unique_antibody_test_id").isNotNull() & F.col("blood_sample_barcode").isNotNull()
    )
    antibody_df = file_exclude(antibody_df, "blood_test_source_file", blood_files_to_exclude)

    survey_antibody_df, antibody_residuals, survey_antibody_failed = merge_blood(survey_df, antibody_df)

    output_antibody_df_list = [survey_antibody_df, antibody_residuals, survey_antibody_failed]
    output_antibody_table_list = antibody_output_tables

    load_to_data_warehouse_tables(output_antibody_df_list, output_antibody_table_list)

    return survey_antibody_df


@register_pipeline_stage("merge_swab_ETL")
def merge_swab_ETL(
    survey_responses_table: str, swab_table: str, swab_files_to_exclude: List[str], swab_output_tables: List[str]
):
    """
    High level function for joining PCR test result data to survey responses.
    Should be run following the antibody/blood result merge.

    Parameters
    ----------
    survey_responses_table
        name of HIVE table containing survey response records
    swab_table
        name of HIVE table containing PCR/swab result records
    swab_files_to_exclude
        PCR/swab result files that should be excluded from the merge.
        Used to remove files that are found to contain invalid data.
    swab_output_tables
        names of the three output tables:
            1. survey responses and successfully joined results
            2. residual PCR/swab result records, where there was no barcode match to join on
            3. PCR/swab result records that failed to meet the criteria for joining
    """
    survey_df = extract_from_table(survey_responses_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )

    swab_df = extract_from_table(swab_table).where(
        F.col("unique_pcr_test_id").isNotNull() & F.col("swab_sample_barcode").isNotNull()
    )
    swab_df = file_exclude(swab_df, "swab_test_source_file", swab_files_to_exclude)

    swab_df = swab_df.dropDuplicates(subset=[column for column in swab_df.columns if column != "swab_test_source_file"])

    survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed = merge_swab(survey_df, swab_df)
    output_swab_df_list = [survey_antibody_swab_df, antibody_swab_residuals, survey_antibody_swab_failed]
    load_to_data_warehouse_tables(output_swab_df_list, swab_output_tables)

    return survey_antibody_swab_df


@register_pipeline_stage("join_vaccination_data")
def join_vaccination_data(participant_records_table, nims_table, vaccination_data_table):
    """
    Join NIMS vaccination data onto participant level records and derive vaccination status using NIMS and CIS data.

    Parameters
    ----------
    participant_records_table
        input table containing participant level records to join
    nims_table
        nims table containing records to be joined to participant table
    vaccination_data_table
        output table name for the joined nims and participant table
    """
    participant_df = extract_from_table(participant_records_table)
    nims_df = extract_from_table(nims_table)
    nims_df = nims_transformations(nims_df)

    participant_df = participant_df.join(nims_df, on="participant_id", how="left")
    participant_df = derive_overall_vaccination(participant_df)

    update_table(participant_df, vaccination_data_table, mode_overide="overwrite")


@register_pipeline_stage("impute_demographic_columns")
def impute_demographic_columns(
    survey_responses_table: str,
    imputed_values_table: str,
    survey_responses_imputed_table: str,
    key_columns: List[str],
):
    """
    Imputes values for key demographic columns.
    Applies filling forward for listed columns. Specific imputations are then used for sex, ethnicity and date of birth.

    Parameters
    ----------
    survey_responses_table
        name of HIVE table containing survey responses for imputation, containing `key_columns`
    imputed_values_table
        name of HIVE table containing previously imputed values
    survey_responses_imputed_table
        name of HIVE table to write survey responses following imputation
    key_columns
        names of key demographic columns to be filled forwards
    """
    imputed_value_lookup_df = None
    if check_table_exists(imputed_values_table):
        imputed_value_lookup_df = extract_from_table(imputed_values_table)
    df = extract_from_table(survey_responses_table)

    key_columns_imputed_df = impute_key_columns(
        df, imputed_value_lookup_df, key_columns, get_config().get("imputation_log_directory", "./")
    )
    imputed_values_df = key_columns_imputed_df.filter(
        reduce(
            lambda col_1, col_2: col_1 | col_2,
            (F.col(f"{column}_imputation_method").isNotNull() for column in key_columns),
        )
    )

    lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in key_columns])
    imputed_values = imputed_values_df.select(
        "participant_id",
        *lookup_columns,
    )
    df_with_imputed_values = df.drop(*key_columns).join(key_columns_imputed_df, on="participant_id", how="left")

    update_table(imputed_values, imputed_values_table)
    update_table(df_with_imputed_values, survey_responses_imputed_table, "overwrite")


@register_pipeline_stage("calculate_household_level_populations")
def calculate_household_level_populations(
    address_lookup, cis_phase_lookup, country_lookup, postcode_lookup, household_level_populations_table
):
    files = {
        "address_lookup": {"file": address_lookup, "type": "path"},
        "cis_phase_lookup": {"file": cis_phase_lookup, "type": "path"},
        "country_lookup": {"file": country_lookup, "type": "path"},
        "postcode_lookup": {"file": postcode_lookup, "type": "path"},
    }
    dfs = extract_df_list(files)
    dfs = prepare_auxillary_data(dfs)

    household_info_df = household_level_populations(
        dfs["address_lookup"],
        dfs["postcode_lookup"],
        dfs["cis_phase_lookup"],
        dfs["country_lookup"],
    )
    update_table(household_info_df, household_level_populations_table, mode_overide="overwrite")


@register_pipeline_stage("join_geographic_data")
def join_geographic_data(
    geographic_table: str, survey_responses_table: str, geographic_responses_table: str, id_column: str
):
    """
    Join weights file onto survey data by household id.

    Parameters
    ----------
    geographic_table
        input table name for household data with geographic data
    survey_responses_table
        input table for individual participant responses
    geographic_responses_table
        output table name for joined survey responses and household geographic data
    id_column
        column containing id to join the 2 input tables
    """
    weights_df = extract_from_table(geographic_table)
    survey_responses_df = extract_from_table(survey_responses_table)
    geographic_survey_df = survey_responses_df.join(weights_df, on=id_column, how="left")
    update_table(geographic_survey_df, geographic_responses_table)


@register_pipeline_stage("geography_and_imputation_logic")
def process_post_merge(
    imputed_antibody_swab_table: str,
    response_records_table: str,
    invalid_response_records_table: str,
    key_columns: List[str],
):
    df_with_imputed_values = extract_from_table(imputed_antibody_swab_table)
    df_with_imputed_values = merge_dependent_transform(df_with_imputed_values)

    imputation_columns = chain(
        *[(column, f"{column}_imputation_method", f"{column}_is_imputed") for column in key_columns]
    )
    response_level_records_df = df_with_imputed_values.drop(*imputation_columns)

    response_level_records_df, response_level_records_filtered_df = filter_response_records(
        response_level_records_df, "visit_datetime"
    )

    multigeneration_df = assign_multigeneration(
        df=response_level_records_df,
        column_name_to_assign="multigen",
        participant_id_column="participant_id",
        household_id_column="ons_household_id",
        visit_date_column="visit_date_string",
        date_of_birth_column="date_of_birth",
        country_column="country_name",
    )

    update_table(multigeneration_df, "multigeneration_table", mode_overide="overwrite")
    update_table(response_level_records_df, response_records_table, mode_overide="overwrite")
    update_table(response_level_records_filtered_df, invalid_response_records_table, mode_overide=None)


@register_pipeline_stage("report")
def report(
    unique_id_column: str,
    validation_failure_flag_column: str,
    duplicate_count_column_name: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
    filtered_survey_responses_table: str,
    output_directory: str,
):
    """
    Create a excel spreadsheet with multiple sheets to summarise key data from various
    tables regarding the running of the pipeline; using overall and most recent statistics.
    Parameters
    ----------
    unique_id_column
        column that should hold unique id for each row in responses file
    validation_failure_flag_column
        name of the column containing the previously created to containt validation error messages
        name should match that created in validate_survey_responses stage
    duplicate_count_column_name
        name of the column containing the previously created to containt count of rows that repeat
        on the responses table. name should match that created in validate_survey_responses stage
    valid_survey_responses_table
        table name of hdfs table of survey responses passing validation checks
    invalid_survey_responses_table
        table name of hdfs table of survey responses failing validation checks
    filtered_survey_responses_table
    output_directory
        output folder location to store the report
    """
    valid_df = extract_from_table(valid_survey_responses_table)
    invalid_df = extract_from_table(invalid_survey_responses_table)
    filtered_df = extract_from_table(filtered_survey_responses_table)
    processed_file_log = extract_from_table("processed_filenames")

    invalid_files_count = 0
    if check_table_exists("error_file_log"):
        invalid_files_log = extract_from_table("error_file_log")
        invalid_files_count = invalid_files_log.count()

    valid_survey_responses_count = valid_df.count()
    invalid_survey_responses_count = invalid_df.count()
    filtered_survey_responses_count = filtered_df.count()

    valid_df_errors = valid_df.select(unique_id_column, validation_failure_flag_column)
    invalid_df_errors = invalid_df.select(unique_id_column, validation_failure_flag_column)

    valid_df_errors = (
        valid_df_errors.withColumn("Validation check failures", F.explode(validation_failure_flag_column))
        .groupBy("Validation check failures")
        .count()
    )
    invalid_df_errors = (
        invalid_df_errors.withColumn("Validation check failures", F.explode(validation_failure_flag_column))
        .groupBy("Validation check failures")
        .count()
    )

    duplicated_df = valid_df.select(unique_id_column, duplicate_count_column_name).filter(
        F.col(duplicate_count_column_name) > 1
    )

    counts_df = pd.DataFrame(
        {
            "dataset": [
                "invalid input files",
                "valid survey responses",
                "invalid survey responses",
                "filtered survey responses",
                *list(processed_file_log.select("processed_filename").distinct().rdd.flatMap(lambda x: x).collect()),
            ],
            "count": [
                invalid_files_count,
                valid_survey_responses_count,
                filtered_survey_responses_count,
                invalid_survey_responses_count,
                *list(processed_file_log.select("file_row_count").distinct().rdd.flatMap(lambda x: x).collect()),
            ],
        }
    )

    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        counts_df.to_excel(writer, sheet_name="dataset totals", index=False)
        valid_df_errors.toPandas().to_excel(
            writer, sheet_name="validation check failures in valid dataset", index=False
        )
        invalid_df_errors.toPandas().to_excel(
            writer, sheet_name="validation check failures in invalid dataset", index=False
        )
        duplicated_df.toPandas().to_excel(writer, sheet_name="duplicated record summary", index=False)

    write_string_to_file(
        output.getbuffer(), f"{output_directory}/report_output_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    )


@register_pipeline_stage("record_level_interface")
def record_level_interface(
    survey_responses_table: str,
    csv_editing_file: str,
    unique_id_column: str,
    unique_id_list: List,
    edited_survey_responses_table: str,
    filtered_survey_responses_table: str,
):
    """
    This stage does two type of edits in a given table from HIVE given an unique_id column.
    Either value level editing or filtering editing.

    Parameters
    ----------
    survey_responses_table
        table in which editing happens
    csv_editing_file
        defines the editing from old values to new values in the HIVE tables
        Columns expected
            - unique id
            - column name to edit
            - old value
            - new value
    unique_id_column
        unique id that will be edited
    unique_id_list
        list of ids to be filtered
    edited_survey_responses_table
        Hive table
    filtered_survey_responses_table
        Hive table when they have been filtered out from survey responses
    """
    input_df = extract_from_table(survey_responses_table)
    edited_df = update_from_csv_lookup(df=input_df, csv_filepath=csv_editing_file, id_column=unique_id_column)
    update_table(edited_df, edited_survey_responses_table, "overwrite")

    filtered_df = edited_df.filter(F.col(unique_id_column).isin(unique_id_list))
    update_table(filtered_df, filtered_survey_responses_table, "overwrite")


@register_pipeline_stage("tables_to_csv")
def tables_to_csv(
    outgoing_directory,
    tables_to_csv_config_file,
    category_map,
    dry_run=False,
):
    """
    Writes data from an existing HIVE table to csv output, including mapping of column names and values.
    Takes a yaml file in which HIVE table name and csv table name are defined as well as columns to be
    included in the csv file by a select statement.
    Optionally also point to an update map to be used for the variable name mapping of these outputs.

    Parameters
    ----------
    outgoing_directory
        path to write output CSV files on HDFS
    tables_to_csv_config_file
        path to YAML config file to define input tables and output CSV names
    category_map
        name of the category map for converting category strings to integers
    dry_run
        when set to True, will delete files after they are written (for testing). Default is False.
    """
    output_datetime = datetime.today()
    output_datetime_str = output_datetime.strftime("%Y%m%d_%H%M%S")

    file_directory = Path(outgoing_directory) / output_datetime_str
    manifest = Manifest(outgoing_directory, pipeline_run_datetime=output_datetime, dry_run=dry_run)
    category_map_dictionary = category_maps[category_map]

    config_file = get_secondary_config(tables_to_csv_config_file)

    for table in config_file["create_tables"]:
        df = extract_from_table(table["table_name"]).select(*[element for element in table["column_name_map"].keys()])
        df = map_output_values_and_column_names(df, table["column_name_map"], category_map_dictionary)
        file_path = file_directory / f"{table['output_file_name']}_{output_datetime_str}"
        write_csv_rename(df, file_path)
        file_path = file_path.with_suffix(".csv")
        header_string = read_header(file_path)

        manifest.add_file(
            relative_file_path=file_path.relative_to(outgoing_directory).as_posix(),
            column_header=header_string,
            validate_col_name_length=False,
        )
    manifest.write_manifest()


@register_pipeline_stage("sample_file_ETL")
def sample_file_ETL(
    household_level_populations_table,
    new_sample_file,
    tranche,
    cis_phase_lookup,
    postcode_lookup,
    table_or_path,
    old_sample_file,
    design_weight_table,
):
    files = {
        "postcode_lookup": {"file": postcode_lookup, "type": "path"},
        "cis_phase_lookup": {"file": cis_phase_lookup, "type": "path"},
        "new_sample_file": {"file": new_sample_file, "type": "path"},
        "old_sample_file": {"file": old_sample_file, "type": table_or_path},
        "tranche": {"file": tranche, "type": "path"},
    }
    dfs = extract_df_list(files)
    dfs = prepare_auxillary_data(dfs)
    dfs["household_level_populations"] = extract_from_table(household_level_populations_table)
    design_weights = generate_weights(dfs)
    update_table(design_weights, design_weight_table, mode_overide="overwrite")


@register_pipeline_stage("calculate_individual_level_population_totals")
def population_projection(
    population_projection_previous: str,
    population_projection_current: str,
    month: int,
    year: int,
    aps_lookup: str,
    table_or_path: str,
    population_totals_table: str,
    population_projections_table: str,
):
    files = {
        "population_projection_current": {"file": population_projection_current, "type": "path"},
        "aps_lookup": {"file": aps_lookup, "type": "path"},
        "population_projection_previous": {"file": population_projection_previous, "type": table_or_path},
    }
    dfs = extract_df_list(files)
    populations_for_calibration, population_projections = proccess_population_projection_df(
        dfs=dfs, month=month, year=year
    )
    update_table(populations_for_calibration, population_totals_table, mode_overide="overwrite")
    update_table(population_projections, population_projections_table, mode_overide="overwrite")


@register_pipeline_stage("pre_calibration")
def pre_calibration(
    design_weight_table,
    individual_level_populations_for_non_response_adjustment_table,
    survey_response_table,
    responses_pre_calibration_table,
    pre_calibration_config_path,
):
    """
    Survey data broken down in different datasets is merged with household_samples_dataset
    Non-response adjustment is calculated and the design weights
    are adjusted by the non-response rates producing desgin weights adjusted.
    Calibration variables are calculated and all the files(dataframes) are written to HIVE
    for the weight calibration
    At the end of this processing stage 24 datasets (files will be produced): 6 datasets for each country

    Parameters
    ----------
    design_weight_table
        name of HIVE table containing household level design weights
    individual_level_populations_for_non_response_adjustment_table
        name of HIVE table containing populations for non-response adjustment
    survey_response_table
        name of HIVE table containing survey responses
    responses_pre_calibration_table
        name of HIVE table to write data for weight calibration
    pre_calibration_config_path
        path to YAML pre-calibration config file
    """
    pre_calibration_config = get_secondary_config(pre_calibration_config_path)
    household_level_with_design_weights = extract_from_table(design_weight_table)
    population_by_country = extract_from_table(individual_level_populations_for_non_response_adjustment_table)

    survey_response = extract_from_table(survey_response_table)

    survey_response = survey_response.select(
        "ons_household_id",
        "participant_id",
        "sex",
        "age_at_visit",
        "ethnicity_white",
    )

    population_by_country = population_by_country.select(
        # "region_code",
        "country_name_12",
        "population_country_swab",
        "population_country_antibodies",
    ).distinct()

    df_for_calibration = pre_calibration_high_level(
        df_survey=survey_response,
        df_dweights=household_level_with_design_weights,
        df_country=population_by_country,
        pre_calibration_config=pre_calibration_config,
    )
    update_table(df_for_calibration, responses_pre_calibration_table, mode_overide="overwrite")
