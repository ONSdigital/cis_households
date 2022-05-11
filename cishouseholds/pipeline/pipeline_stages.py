from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import List
from typing import Union

import pandas as pd
from pyspark.sql import functions as F

from cishouseholds.derive import aggregated_output_groupby
from cishouseholds.derive import aggregated_output_window
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_multigeneration
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.hdfs_utils import read_header
from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.merge import join_assayed_bloods
from cishouseholds.merge import union_dataframes_to_hive
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.category_map import category_maps
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.generate_outputs import map_output_values_and_column_names
from cishouseholds.pipeline.generate_outputs import write_csv_rename
from cishouseholds.pipeline.input_file_processing import extract_from_table
from cishouseholds.pipeline.input_file_processing import extract_input_data
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.input_file_processing import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import column_name_maps
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.load import get_run_id
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.manifest import Manifest
from cishouseholds.pipeline.merge_antibody_swab_ETL import load_to_data_warehouse_tables
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood_process_filtering
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood_process_preparation
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_blood_xtox_flag
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab_process_filtering
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab_process_preparation
from cishouseholds.pipeline.merge_antibody_swab_ETL import merge_swab_xtox_flag
from cishouseholds.pipeline.merge_process import merge_process_validation
from cishouseholds.pipeline.post_merge_processing import derive_overall_vaccination
from cishouseholds.pipeline.post_merge_processing import impute_key_columns
from cishouseholds.pipeline.post_merge_processing import nims_transformations
from cishouseholds.pipeline.reporting import dfs_to_bytes_excel
from cishouseholds.pipeline.reporting import multiple_visit_1_day
from cishouseholds.pipeline.survey_responses_version_2_ETL import fill_forwards_transformations
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_cleaning
from cishouseholds.pipeline.survey_responses_version_2_ETL import union_dependent_derivations
from cishouseholds.pipeline.validation_ETL import validation_ETL
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_files
from cishouseholds.weights.edit import aps_value_map
from cishouseholds.weights.edit import recode_column_values
from cishouseholds.weights.population_projections import proccess_population_projection_df
from cishouseholds.weights.pre_calibration import pre_calibration_high_level
from cishouseholds.weights.weights import generate_weights
from cishouseholds.weights.weights import household_level_populations
from dummy_data_generation.generate_data import generate_digital_data
from dummy_data_generation.generate_data import generate_historic_bloods_data
from dummy_data_generation.generate_data import generate_nims_table
from dummy_data_generation.generate_data import generate_ons_gl_report_data
from dummy_data_generation.generate_data import generate_survey_v0_data
from dummy_data_generation.generate_data import generate_survey_v1_data
from dummy_data_generation.generate_data import generate_survey_v2_data
from dummy_data_generation.generate_data import generate_unioxf_medtest_data

# from cishouseholds.pipeline.input_variable_names import cis_phase_lookup_column_map
# from cishouseholds.pipeline.validation_schema import cis_phase_schema

pipeline_stages = {}


def register_pipeline_stage(key):
    """Decorator to register a pipeline stage function."""

    def _add_pipeline_stage(func):
        pipeline_stages[key] = func
        return func

    return _add_pipeline_stage


@register_pipeline_stage("blind_csv_to_table")
def blind_csv_to_table(path: str, table_name: str):
    """
    Convert a single csv file to a HDFS table by inferring a schema
    """
    df = extract_input_data(path, None, ",")
    df = update_table(df, table_name, "overwrite")


@register_pipeline_stage("csv_to_table")
def csv_to_table(file_operations: list):
    """
    Convert a list of csv files into a HDFS table
    """
    for file in file_operations:
        if file["schema"] not in validation_schemas:
            raise ValueError(file["schema"] + " schema does not exists")
        schema = validation_schemas[file["schema"]]
        column_map = column_name_maps[file["column_map"]] if file["column_map"] in column_name_maps else None
        df = extract_lookup_csv(
            file["path"],
            schema,
            column_map,
            file["drop_not_found"],
        )
        print("    created table:" + file["table_name"])  # functional
        update_table(df, file["table_name"], "overwrite")


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
    digital_survey_dir = raw_dir / "responses_digital"
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
        digital_survey_dir,
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
    generate_digital_data(
        directory=digital_survey_dir,
        file_date=file_date,
        records=50,
        swab_barcodes=swab_barcode,
        blood_barcodes=blood_barcode,
    )

    participant_ids = v2["Participant_id"].unique().tolist()

    generate_nims_table(get_full_table_name("cis_nims_20210101"), participant_ids)


def generate_input_processing_function(
    stage_name,
    dataset_name,
    id_column,
    validation_schema,
    datetime_column_map,
    transformation_functions,
    source_file_column,
    write_mode="overwrite",
    column_name_map=None,
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
        resource_path,
        dataset_name=dataset_name,
        id_column=id_column,
        latest_only=False,
        start_date=None,
        end_date=None,
        include_processed=False,
        include_invalid=False,
        source_file_column=source_file_column,
        write_mode=write_mode,
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
            include_hadoop_read_write=include_hadoop_read_write,
            resource_path=file_path_list,
            dataset_name=dataset_name,
            id_column=id_column,
            variable_name_map=column_name_map,
            datetime_map=datetime_column_map,
            validation_schema=validation_schema,
            transformation_functions=transformation_functions,
            source_file_column=source_file_column,
            sep=sep,
            cast_to_double_columns_list=cast_to_double_list,
            write_mode=write_mode,
        )
        if include_hadoop_read_write:
            update_table_and_log_source_files(
                df, f"transformed_{dataset_name}", source_file_column, dataset_name, write_mode
            )
        return df

    _inner_function.__name__ = stage_name
    return _inner_function


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files(tables_to_union: List, unioned_survey_responses_table: str):
    """
    Union survey response for v0, v1 and v2, and write to table.
    Parameters
    ----------
    unioned_survey_responses_table
        input tables for extracting each of the transformed survey responses tables
    unioned_survey_responses_table
        output table name for the combine file of all unioned survey responses
    """
    df_list = [extract_from_table(table) for table in tables_to_union]

    union_dataframes_to_hive(unioned_survey_responses_table, df_list)


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
    update_table(unioned_survey_responses, transformed_table, write_mode="overwrite")


@register_pipeline_stage("fill_forwards_stage")
def fill_forwards_stage(unioned_survey_table: str, filled_forwards_table: str):
    df = extract_from_table(unioned_survey_table)
    df = fill_forwards_transformations(df)
    update_table(df, filled_forwards_table, write_mode="overwrite")


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
    update_table(valid_survey_responses, valid_survey_responses_table, write_mode="overwrite")
    update_table(erroneous_survey_responses, invalid_survey_responses_table, write_mode="overwrite")


@register_pipeline_stage("lookup_based_editing")
def lookup_based_editing(
    input_table: str,
    cohort_lookup_path: str,
    travel_countries_lookup_path: str,
    rural_urban_lookup_path: str,
    tenure_group_path: str,
    edited_table: str,
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
    spark = get_or_create_spark_session()
    df = extract_from_table(input_table)

    cohort_lookup = spark.read.csv(
        cohort_lookup_path, header=True, schema="participant_id string, new_cohort string, old_cohort string"
    ).withColumnRenamed("participant_id", "cohort_participant_id")

    travel_countries_lookup = spark.read.csv(
        travel_countries_lookup_path,
        header=True,
        schema="been_outside_uk_last_country_old string, been_outside_uk_last_country_new string",
    )
    rural_urban_lookup_df = spark.read.csv(
        rural_urban_lookup_path,
        header=True,
        schema="""
            lower_super_output_area_code_11 string,
            cis_rural_urban_classification string,
            rural_urban_classification_11 string
        """,
    ).drop(
        "rural_urban_classification_11"
    )  # Prefer version from sample
    df = df.join(
        F.broadcast(cohort_lookup),
        how="left",
        on=((df.participant_id == cohort_lookup.cohort_participant_id) & (df.study_cohort == cohort_lookup.old_cohort)),
    )
    df = df.withColumn("study_cohort", F.coalesce(F.col("new_cohort"), F.col("study_cohort"))).drop(
        "new_cohort", "old_cohort"
    )
    df = df.join(
        F.broadcast(travel_countries_lookup),
        how="left",
        on=df.been_outside_uk_last_country == travel_countries_lookup.been_outside_uk_last_country_old,
    )
    df = df.withColumn(
        "been_outside_uk_last_country",
        F.coalesce(F.col("been_outside_uk_last_country_new"), F.col("been_outside_uk_last_country")),
    ).drop("been_outside_uk_last_country_old", "been_outside_uk_last_country_new")

    if "lower_super_output_area_code_11" in df.columns:
        df = df.join(
            F.broadcast(rural_urban_lookup_df),
            how="left",
            on="lower_super_output_area_code_11",
        )
    tenure_group = spark.read.csv(tenure_group_path, header=True).select(
        "UAC", "numAdult", "numChild", "dvhsize", "tenure_group"
    )
    for key, value in column_name_maps["tenure_group_variable_map"].items():
        tenure_group = tenure_group.withColumnRenamed(key, value)

    df = df.join(tenure_group, on=(df["ons_household_id"] == tenure_group["UAC"]), how="left").drop("UAC")

    update_table(df, edited_table, write_mode="overwrite")


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

    update_table(blood_df, joined_antibody_test_result_table, write_mode="overwrite")
    update_table(failed_blood_join_df, failed_join_table, write_mode="overwrite")


@register_pipeline_stage("merge_blood_ETL")
def merge_blood_ETL(
    input_survey_responses_table,
    input_antibody_table,
    blood_files_to_exclude,
    merged_1to1_table,
    joined_table,
    xtox_flagged_table,
    validated_table,
    merged_responses_antibody_data,
    antibody_merge_residuals,
    antibody_merge_failed_records,
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
    survey_df = extract_from_table(input_survey_responses_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    antibody_df = extract_from_table(input_antibody_table).where(
        F.col("unique_antibody_test_id").isNotNull() & F.col("blood_sample_barcode").isNotNull()
    )
    df_1to1, df = merge_blood_process_preparation(
        survey_df,
        antibody_df,
        blood_files_to_exclude,
    )
    update_table(df, joined_table, write_mode="overwrite")
    update_table(df_1to1, merged_1to1_table, write_mode="overwrite")  # Fastforward 1to1 succesful matches

    df = extract_from_table(joined_table)
    df = merge_blood_xtox_flag(df)
    update_table(df=df, table_name=xtox_flagged_table, write_mode="overwrite")

    df = extract_from_table(xtox_flagged_table)
    df = merge_process_validation(
        df=df,
        merge_type="antibody",
        barcode_column_name="blood_sample_barcode",
    )
    update_table(df=df, table_name=validated_table, write_mode="overwrite")

    df = extract_from_table(validated_table)

    (
        merged_responses_antibody_df,
        antibody_merge_residuals_df,
        antibody_merge_failed_records_df,
    ) = merge_blood_process_filtering(df)

    # load back 1to1 fastforwarded matches
    merged_1to1_df = extract_from_table(merged_1to1_table)
    merged_responses_antibody_df = union_multiple_tables([merged_1to1_df, merged_responses_antibody_df])

    load_to_data_warehouse_tables(
        [
            merged_responses_antibody_df,
            antibody_merge_residuals_df,
            antibody_merge_failed_records_df,
        ],
        [merged_responses_antibody_data, antibody_merge_residuals, antibody_merge_failed_records],
    )


@register_pipeline_stage("merge_swab_ETL")
def merge_swab_ETL(
    input_survey_responses_table,
    input_swab_table,
    swab_files_to_exclude,
    merged_1to1_table,
    joined_table,
    xtox_flagged_table,
    validated_table,
    merged_responses_antibody_swab_data,
    swab_merge_residuals,
    swab_merge_failed_records,
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
    survey_df = extract_from_table(input_survey_responses_table).where(
        F.col("unique_participant_response_id").isNotNull() & (F.col("unique_participant_response_id") != "")
    )
    swab_df = extract_from_table(input_swab_table).where(
        F.col("unique_pcr_test_id").isNotNull() & F.col("swab_sample_barcode").isNotNull()
    )
    df_1to1, df = merge_swab_process_preparation(
        survey_df,
        swab_df,
        swab_files_to_exclude,
    )
    update_table(df=df_1to1, table_name=merged_1to1_table, write_mode="overwrite")
    update_table(df=df, table_name=joined_table, write_mode="overwrite")

    df = extract_from_table(joined_table)
    df = merge_swab_xtox_flag(df)
    update_table(df=df, table_name=xtox_flagged_table, write_mode="overwrite")

    df = extract_from_table(xtox_flagged_table)
    df = merge_process_validation(
        df=df,
        merge_type="swab",
        barcode_column_name="swab_sample_barcode",
    )
    update_table(df=df, table_name=validated_table, write_mode="overwrite")

    df = extract_from_table(validated_table)
    (
        merged_responses_antibody_swab_df,
        swab_merge_residuals_df,
        swab_merge_failed_records_df,
    ) = merge_swab_process_filtering(df)

    # load back 1to1 fastforwarded matches
    merged_1to1_df = extract_from_table(merged_1to1_table)
    merged_responses_antibody_swab_df = union_multiple_tables([merged_1to1_df, merged_responses_antibody_swab_df])

    load_to_data_warehouse_tables(
        [
            merged_responses_antibody_swab_df,
            swab_merge_residuals_df,
            swab_merge_failed_records_df,
        ],
        [
            merged_responses_antibody_swab_data,
            swab_merge_residuals,
            swab_merge_failed_records,
        ],
    )


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

    update_table(participant_df, vaccination_data_table, write_mode="overwrite")


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
    # imputed_values_df = key_columns_imputed_df.filter(
    #     reduce(
    #         lambda col_1, col_2: col_1 | col_2,
    #         (F.col(f"{column}_imputation_method").isNotNull() for column in key_columns),
    #     )
    # )

    # lookup_columns = chain(*[(column, f"{column}_imputation_method") for column in key_columns])
    # imputed_values = imputed_values_df.select(
    #     "participant_id",
    #     *lookup_columns,
    # )
    df_with_imputed_values = df.drop(*key_columns).join(
        F.broadcast(key_columns_imputed_df), on="participant_id", how="left"
    )

    # update_table(imputed_values, imputed_values_table)
    update_table(df_with_imputed_values, survey_responses_imputed_table, "overwrite")


@register_pipeline_stage("calculate_household_level_populations")
def calculate_household_level_populations(
    address_lookup_table,
    postcode_lookup_table,
    lsoa_cis_lookup_table,
    country_lookup_table,
    household_level_populations_table,
):
    address_lookup_df = extract_from_table(address_lookup_table).select("unique_property_reference_code", "postcode")
    postcode_lookup_df = (
        extract_from_table(postcode_lookup_table)
        .select("postcode", "lower_super_output_area_code_11", "country_code_12")
        .distinct()
    )
    lsoa_cis_lookup_df = (
        extract_from_table(lsoa_cis_lookup_table)
        .select("lower_super_output_area_code_11", "cis_area_code_20")
        .distinct()
    )
    country_lookup_df = extract_from_table(country_lookup_table).select("country_code_12", "country_name_12").distinct()

    household_info_df = household_level_populations(
        address_lookup_df, postcode_lookup_df, lsoa_cis_lookup_df, country_lookup_df
    )
    update_table(household_info_df, household_level_populations_table, write_mode="overwrite")


@register_pipeline_stage("join_geographic_data")
def join_geographic_data(
    geographic_table: str,
    survey_responses_table: str,
    geographic_responses_table: str,
    id_column: str,
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
    design_weights_df = extract_from_table(geographic_table)
    survey_responses_df = extract_from_table(survey_responses_table)
    geographic_survey_df = survey_responses_df.drop("postcode", "region_code").join(
        design_weights_df, on=id_column, how="left"
    )
    update_table(geographic_survey_df, geographic_responses_table, write_mode="overwrite")


@register_pipeline_stage("geography_and_imputation_dependent_logic")
def geography_and_imputation_dependent_processing(
    imputed_responses_table: str,
    output_imputed_responses_table: str,
):
    """
    Apply processing that depends on the imputation and geographic columns being created
    Parameters
    -----------
    imputed_responses_table
    response_records_table
    invalid_response_records_table
    output_imputed_responses_table
    key_columns
    """
    df_with_imputed_values = extract_from_table(imputed_responses_table)

    ethnicity_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveller", "Any other white background"],
        "Asian": [
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Any other Asian background",
        ],
        "Black": ["Black,Caribbean,African-African", "Black,Caribbean,Afro-Caribbean", "Any other Black background"],
        "Mixed": [
            "Mixed-White & Black Caribbean",
            "Mixed-White & Black African",
            "Mixed-White & Asian",
            "Any other Mixed background",
        ],
        "Other": ["Other ethnic group-Arab", "Any other ethnic group"],
    }

    df_with_imputed_values = assign_column_from_mapped_list_key(
        df=df_with_imputed_values,
        column_name_to_assign="ethnicity_group_corrected",
        reference_column="ethnicity",
        map=ethnicity_map,
    )
    df_with_imputed_values = assign_ethnicity_white(
        df_with_imputed_values,
        column_name_to_assign="ethnicity_white_corrected",
        ethnicity_group_column_name="ethnicity_group_corrected",
    )

    df_with_imputed_values = assign_multigeneration(
        df=df_with_imputed_values,
        column_name_to_assign="multigen",
        participant_id_column="participant_id",
        household_id_column="ons_household_id",
        visit_date_column="visit_datetime",
        date_of_birth_column="date_of_birth",
        country_column="country_name_12",
    )
    update_table(df_with_imputed_values, output_imputed_responses_table, write_mode="overwrite")


@register_pipeline_stage("report")
def report(
    unique_id_column: str,
    validation_failure_flag_column: str,
    duplicate_count_column_name: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
    output_directory: str,
    tables_to_count: List[str],
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
    output_directory
        output folder location to store the report
    """
    valid_df = extract_from_table(valid_survey_responses_table)
    invalid_df = extract_from_table(invalid_survey_responses_table)

    processed_file_log = extract_from_table("processed_filenames")

    invalid_files_count = 0
    if check_table_exists("error_file_log"):
        invalid_files_log = extract_from_table("error_file_log")
        invalid_files_count = invalid_files_log.filter(F.col("run_id") == get_run_id()).count()

    valid_survey_responses_count = valid_df.count()
    invalid_survey_responses_count = invalid_df.count()

    table_counts = {
        "error_file_log": invalid_files_count,
        valid_survey_responses_table: valid_survey_responses_count,
        invalid_survey_responses_table: invalid_survey_responses_count,
    }
    for table_name in tables_to_count:
        if check_table_exists(table_name):
            table = extract_from_table(table_name)
            table_counts[table_name] = table.count()
        else:
            table_counts[table_name] = "Table not found"

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
                *list(table_counts.keys()),
            ],
            "count": [
                *list(table_counts.values()),
            ],
        }
    )

    output = BytesIO()
    datasets = list(processed_file_log.select("dataset_name").distinct().rdd.flatMap(lambda x: x).collect())
    with pd.ExcelWriter(output) as writer:
        for dataset in datasets:
            processed_files_df = (
                processed_file_log.filter(F.col("dataset_name") == dataset)
                .select("processed_filename", "file_row_count")
                .orderBy("processed_filename")
                .distinct()
                .toPandas()
            )
            processed_file_names = [name.split("/")[-1] for name in processed_files_df["processed_filename"]]
            processed_file_counts = processed_files_df["file_row_count"]
            individual_counts_df = pd.DataFrame({"dataset": processed_file_names, "count": processed_file_counts})
            name = f"{dataset}"
            individual_counts_df.to_excel(writer, sheet_name=name, index=False)

        counts_df.to_excel(writer, sheet_name="dataset totals", index=False)
        valid_df_errors.toPandas().to_excel(writer, sheet_name="validation fails valid data", index=False)
        invalid_df_errors.toPandas().to_excel(writer, sheet_name="validation fails invalid data", index=False)
        duplicated_df.toPandas().to_excel(writer, sheet_name="duplicated record summary", index=False)

    write_string_to_file(
        output.getbuffer(), f"{output_directory}/report_output_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    )


@register_pipeline_stage("report_iqvia")
def report_iqvia(swab_residuals_table: str, blood_residuals_table: str, survey_response_table):
    """ " """
    swab_residuals_df = extract_from_table(swab_residuals_table)
    blood_residuals_df = extract_from_table(blood_residuals_table)
    swab_residuals_df = swab_residuals_df.filter(F.col("pcr_result_classification") != "positive")
    survey_response_table = extract_from_table(survey_response_table)

    modified_barcodes_df = survey_response_table.filter(
        F.col("swab_sample_barcode_edited_flag") == 1 | F.col("blood_sample_barcode_edited_flag") == 1
    ).select("blood_sample_barcode", "swab_sample_barcode", "")

    multiple_visit_1_day_df = multiple_visit_1_day(
        df=survey_response_table,
        participant_id="participant_id",
        visit_id="visit_id",
        date_column="date",
        datetime_column="datetime",
    )
    sheet_df_map = {
        "unlinked swabs": swab_residuals_df,
        "unlinked bloods": blood_residuals_df,
        "modified barcodes": modified_barcodes_df,
        "participant 1-day multiple visit": multiple_visit_1_day_df,
    }
    output = dfs_to_bytes_excel(sheet_df_map)


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
        HIVE table containing responses to edit
    csv_editing_file
        defines the editing from old values to new values in the HIVE tables
        Columns expected
            - id
            - dataset_name
            - target_column
            - old_value
            - new_value
    unique_id_column
        unique id that will be edited
    unique_id_list
        list of ids to be filtered
    edited_survey_responses_table
        HIVE table to write edited responses
    filtered_survey_responses_table
        HIVE table when they have been filtered out from survey responses
    """
    df = extract_from_table(survey_responses_table)

    filtered_out_df = df.filter(F.col(unique_id_column).isin(unique_id_list))
    update_table(filtered_out_df, filtered_survey_responses_table, "overwrite")

    lookup_df = extract_lookup_csv(csv_editing_file, validation_schemas["csv_lookup_schema"])
    filtered_in_df = df.filter(~F.col(unique_id_column).isin(unique_id_list))
    edited_df = update_from_lookup_df(filtered_in_df, lookup_df, id_column=unique_id_column)
    update_table(edited_df, edited_survey_responses_table, "overwrite")


@register_pipeline_stage("tables_to_csv")
def tables_to_csv(
    outgoing_directory,
    tables_to_csv_config_file,
    category_map,
    sep="|",
    extension=".txt",
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
        write_csv_rename(df, file_path, sep, extension)
        file_path = file_path.with_suffix(extension)
        header_string = read_header(file_path)

        manifest.add_file(
            relative_file_path=file_path.relative_to(outgoing_directory).as_posix(),
            column_header=header_string,
            validate_col_name_length=False,
            sep=sep,
        )
    manifest.write_manifest()


@register_pipeline_stage("sample_file_ETL")
def sample_file_ETL(
    household_level_populations_table,
    old_sample_file,
    new_sample_file,
    tranche,
    postcode_lookup,
    master_sample_file,
    design_weight_table,
    country_lookup,
    lsoa_cis_lookup,
):
    first_run = True if check_table_exists(design_weight_table) else False

    if check_table_exists(design_weight_table):
        first_run = False

    postcode_lookup_df = extract_from_table(postcode_lookup)
    lsoa_cis_lookup_df = extract_from_table(lsoa_cis_lookup)
    country_lookup_df = extract_from_table(country_lookup)
    old_sample_df = extract_from_table(old_sample_file)
    master_sample_df = extract_from_table(master_sample_file)

    new_sample_df = extract_lookup_csv(
        new_sample_file,
        validation_schemas["new_sample_file_schema"],
        column_name_maps["new_sample_file_column_map"],
        True,
    )
    tranche_df = None
    if tranche is not None:
        tranche_df = extract_lookup_csv(
            tranche, validation_schemas["tranche_schema"], column_name_maps["tranche_column_map"], True
        )

    household_level_populations_df = extract_from_table(household_level_populations_table)
    design_weights = generate_weights(
        household_level_populations_df,
        master_sample_df,
        old_sample_df,
        new_sample_df,
        tranche_df,
        postcode_lookup_df,
        country_lookup_df,
        lsoa_cis_lookup_df,
        first_run,
    )
    update_table(design_weights, design_weight_table, write_mode="overwrite", archive=True)


@register_pipeline_stage("calculate_individual_level_population_totals")
def population_projection(
    population_projection_previous: str,
    population_projection_current: str,
    month: int,
    year: int,
    aps_lookup: str,
    population_totals_table: str,
    population_projections_table: str,
):
    if check_table_exists(population_projections_table):
        population_projection_previous_df = extract_from_table(population_projections_table)
    else:
        population_projection_previous_df = extract_lookup_csv(
            population_projection_previous,
            validation_schemas["population_projection_previous_schema"],
            column_name_maps["population_projection_previous_column_map"],
        )
    population_projection_current_df = extract_lookup_csv(
        population_projection_current,
        validation_schemas["population_projection_current_schema"],
        column_name_maps["population_projection_current_column_map"],
    )
    aps_lookup_df = extract_lookup_csv(
        aps_lookup, validation_schemas["aps_schema"], column_name_maps["aps_column_map"], True
    )
    aps_lookup_df = recode_column_values(aps_lookup_df, aps_value_map)
    populations_for_calibration, population_projections = proccess_population_projection_df(
        population_projection_previous_df, population_projection_current_df, aps_lookup_df, month, year
    )
    update_table(populations_for_calibration, population_totals_table, write_mode="overwrite")
    update_table(population_projections, population_projections_table, write_mode="append")


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
    update_table(df_for_calibration, responses_pre_calibration_table, write_mode="overwrite")


@register_pipeline_stage("aggregated_output")
def aggregated_output(
    apply_aggregate_type,
    input_table_to_aggregate,
    column_group,
    column_window_list,
    order_window_list,
    apply_function_list,
    column_name_list,
    column_name_to_assign_list,
):
    """
    Parameters
    ----------
    apply_window
    apply_groupby
    aggregated_output
    column_name_to_assign_list
    column_group
    column_window_list
    function_list
    column_list_to_apply_function
    """
    df = extract_from_table(table_name=input_table_to_aggregate)

    if apply_aggregate_type == "groupby":
        df = aggregated_output_groupby(
            df=df,
            column_group=column_group,
            apply_function_list=apply_function_list,
            column_name_list=column_name_list,
            column_name_to_assign_list=column_name_to_assign_list,
        )
    elif apply_aggregate_type == "window":
        df = aggregated_output_window(
            df=df,
            column_window_list=column_window_list,
            column_name_list=column_name_list,
            apply_function_list=apply_function_list,
            column_name_to_assign_list=column_name_to_assign_list,
            order_column_list=order_window_list,
        )
    update_table(
        df=df,
        table_name=f"{input_table_to_aggregate}_{apply_aggregate_type}",
        write_mode="overwrite",
    )
