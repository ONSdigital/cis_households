from datetime import datetime
from functools import reduce
from io import BytesIO
from operator import and_
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import aggregated_output_groupby
from cishouseholds.derive import aggregated_output_window
from cishouseholds.derive import assign_age_group_school_year
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_multigenerational
from cishouseholds.derive import assign_outward_postcode
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import household_level_populations
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import update_from_lookup_df
from cishouseholds.expressions import all_columns_null
from cishouseholds.expressions import any_column_not_null
from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.hdfs_utils import copy
from cishouseholds.hdfs_utils import copy_local_to_hdfs
from cishouseholds.hdfs_utils import create_dir
from cishouseholds.hdfs_utils import isdir
from cishouseholds.hdfs_utils import read_header
from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import post_imputation_wrapper
from cishouseholds.merge import left_join_keep_right
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.design_weights import calculate_design_weights
from cishouseholds.pipeline.generate_outputs import generate_sample
from cishouseholds.pipeline.generate_outputs import map_output_values_and_column_names
from cishouseholds.pipeline.generate_outputs import write_csv_rename
from cishouseholds.pipeline.high_level_transformations import blood_past_positive_transformations
from cishouseholds.pipeline.high_level_transformations import create_formatted_datetime_string_columns
from cishouseholds.pipeline.high_level_transformations import derive_age_based_columns
from cishouseholds.pipeline.high_level_transformations import derive_overall_vaccination
from cishouseholds.pipeline.high_level_transformations import design_weights_lookup_transformations
from cishouseholds.pipeline.high_level_transformations import fill_forward_events_for_key_columns
from cishouseholds.pipeline.high_level_transformations import fill_forwards_transformations
from cishouseholds.pipeline.high_level_transformations import get_differences
from cishouseholds.pipeline.high_level_transformations import impute_key_columns
from cishouseholds.pipeline.high_level_transformations import nims_transformations
from cishouseholds.pipeline.high_level_transformations import ordered_household_id_tranformations
from cishouseholds.pipeline.high_level_transformations import process_healthcare_regex
from cishouseholds.pipeline.high_level_transformations import process_vaccine_regex
from cishouseholds.pipeline.high_level_transformations import reclassify_work_variables
from cishouseholds.pipeline.high_level_transformations import replace_design_weights_transformations
from cishouseholds.pipeline.high_level_transformations import transform_cis_soc_data
from cishouseholds.pipeline.high_level_transformations import transform_from_lookups
from cishouseholds.pipeline.high_level_transformations import union_dependent_cleaning
from cishouseholds.pipeline.high_level_transformations import union_dependent_derivations
from cishouseholds.pipeline.input_file_processing import extract_input_data
from cishouseholds.pipeline.input_file_processing import extract_lookup_csv
from cishouseholds.pipeline.input_file_processing import extract_validate_transform_input_data
from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import delete_tables
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_full_table_name
from cishouseholds.pipeline.load import get_run_id
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.manifest import Manifest
from cishouseholds.pipeline.mapping import category_maps
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.mapping import soc_regex_map
from cishouseholds.pipeline.reporting import count_variable_option
from cishouseholds.pipeline.reporting import generate_error_table
from cishouseholds.pipeline.reporting import generate_lab_report
from cishouseholds.pipeline.timestamp_map import csv_datetime_maps
from cishouseholds.pipeline.validation_calls import validation_ETL
from cishouseholds.pipeline.validation_schema import soc_schema
from cishouseholds.pipeline.validation_schema import validation_schemas  # noqa: F401
from cishouseholds.prediction_checker_class import PredictionChecker
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import check_lookup_table_joined_columns_unique
from cishouseholds.validate import normalise_schema
from cishouseholds.validate import validate_files
from dummy_data_generation.generate_data import generate_cis_soc_data
from dummy_data_generation.generate_data import generate_digital_data
from dummy_data_generation.generate_data import generate_nims_table
from dummy_data_generation.generate_data import generate_survey_v0_data
from dummy_data_generation.generate_data import generate_survey_v1_data
from dummy_data_generation.generate_data import generate_survey_v2_data

# from cishouseholds.pipeline.high_level_transformations import fix_timestamps

pipeline_stages = {}


def register_pipeline_stage(key):
    """Decorator to register a pipeline stage function."""

    def _add_pipeline_stage(func):
        pipeline_stages[key] = func
        return func

    return _add_pipeline_stage


@register_pipeline_stage("blind_csv_to_table")
def blind_csv_to_table(path: str, table_name: str, sep: str = "|"):
    """
    Converts a single csv file to a HIVE table by inferring a schema

    Parameters
    ----------
    path : str
        HSFS or local path for csv file
    table_name : str
        table name to assign to created HIVE table
    sep : str, optional
        separator used in file provided in path, by default "|"
    """
    df = extract_input_data(path, None, sep)
    df = update_table(df, table_name, "overwrite")


@register_pipeline_stage("table_to_table")
def table_to_table(table_name: str, break_lineage: bool = False, alternate_prefix: str = None):
    """
    Extracts a HIVE table, with an alternate prefix, and saves it out with the project prefix

    Parameters
    ----------
    table_name : str
        HIVE table name
    break_lineage : bool
        whether to create a checkpoint on loading the file
    alternate_prefix : str
        alternate prefix to use for input HIVE table
    """
    df = extract_from_table(table_name, break_lineage, alternate_prefix)
    df = update_table(df, table_name, "overwrite")


@register_pipeline_stage("csv_to_table")
def csv_to_table(file_operations: list):
    """
    Converts a list of csv files into HDFS tables. Requires a schema

    Check extract_lookup_csv for parameter documentation.
    """
    for file in file_operations:
        if file["schema"] not in validation_schemas:
            raise ValueError(f"Schema doesn't exist: {file['schema']}")
        schema = validation_schemas[file["schema"]]

        if file["column_map"] is not None and file["column_map"] not in column_name_maps:
            raise ValueError(f"Column name map doesn't exist: {file['column_map']}")
        column_map = column_name_maps.get(file["column_map"])

        df = extract_lookup_csv(
            file["path"],
            schema,
            column_map,
            file["drop_not_found"],
        )

        if file.get("datetime_map") is not None and file["datetime_map"] not in csv_datetime_maps:
            raise ValueError(f"CSV datetime map doesn't exist: {file['datetime_map']}")
        if file.get("datetime_map") is not None:
            df = convert_columns_to_timestamps(df, csv_datetime_maps[file["datetime_map"]])
        update_table(df, file["table_name"], "overwrite")
        print("    created table:" + file["table_name"])  # functional


@register_pipeline_stage("backup_files")
def backup_files(file_list: List[str], backup_directory: str):
    # """
    # Backup a list of files on the local or HDFS file system to a HDFS backup directory.
    # """
    """
    Backup a list of files on the local or HDFS file system to a HDFS backup directory.

    Parameters
    ----------
    file_list : List[str]
    backup_directory : str

    Raises
    ------
    FileNotFoundError
        if directory cannot be created on HDFS
    """
    storage_dir = (
        backup_directory + "/" + get_config()["storage"]["table_prefix"] + datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    if not isdir(storage_dir):
        if create_dir(storage_dir):
            print(f"    created dir: {storage_dir}")  # functional
        else:
            raise FileNotFoundError(f"failed to create dir: {storage_dir}")  # functional

    for file_path in file_list:
        new_path = storage_dir + "/" + Path(file_path).name
        function = copy_local_to_hdfs
        if "hdfs:///" in file_path:
            function = copy
        if function(file_path, new_path):
            print(f"    backed up {Path(file_path).name} to {storage_dir}")  # functional
        else:
            print(f"    failed to back up {Path(file_path).name} to {storage_dir}")  # functional


@register_pipeline_stage("delete_tables")
def delete_tables_stage(
    ignore_table_prefix: bool = False,
    table_names: Union[str, List[str]] = None,
    prefix: str = None,
    protected_tables: List[str] = [],
    drop_protected_tables: bool = False,
):
    """
    Deletes HIVE tables. For use at the start of a pipeline run, to reset pipeline logs and data.
    Should not be used in production, as all tables may be deleted.
    Use one or more of the optional parameters.

    Parameters
    ----------
    prefix
        boolean to remove all tables with current config table prefix
    table_names
        one or more absolute table names to delete (current config prefix added automatically)
    pattern
        drop tables where table with the current config prefix and name matches pattern in SQL format (e.g. "%_responses_%")
    protected_tables
        list of tables to be protected from any call of this stage
    drop_protected_tables
        boolean to drop protected tables
    """
    delete_tables(prefix, table_names, ignore_table_prefix, protected_tables, drop_protected_tables)


@register_pipeline_stage("generate_dummy_data")
def generate_dummy_data(output_directory):
    """
    Generates dummy input table data for Voyager 0, 1, 2, CIS-Digital, and CIS-SOC-code schemas
    """
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
    cis_soc_directory = raw_dir / "cis_soc"

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
    file_date = datetime.strftime(file_datetime, format="%Y%m%d")

    generate_cis_soc_data(directory=cis_soc_directory, file_date=file_date, records=50)

    generate_survey_v0_data(directory=survey_dir, file_date=file_date, records=50, swab_barcodes=[], blood_barcodes=[])
    generate_survey_v1_data(directory=survey_dir, file_date=file_date, records=50, swab_barcodes=[], blood_barcodes=[])
    v2 = generate_survey_v2_data(
        directory=survey_dir, file_date=file_date, records=50, swab_barcodes=[], blood_barcodes=[]
    )
    generate_digital_data(
        directory=digital_survey_dir,
        file_date=file_date,
        records=50,
        swab_barcodes=[],
        blood_barcodes=[],
    )

    participant_ids = v2["Participant_id"].unique().tolist()

    generate_nims_table(get_full_table_name("cis_nims_20210101"), participant_ids)


@register_pipeline_stage("process_soc_deltas")
def process_soc_deltas(
    soc_file_pattern: str,
    source_file_column: str,
    soc_lookup_table: str,
    coding_errors_table: str,
    inconsistencies_resolution_table: str,
    include_processed=False,
    include_invalid=False,
    latest_only=False,
    start_date=None,
    end_date=None,
):
    """
    Process soc data and combine result with survey responses data, requires a inconsistencies_reoslution_table
    to exist as a HIVE table

    Parameters
    ----------
    soc_file_pattern : str
        HDFS or local file path pattern for relevant soc Batch* files
    source_file_column : str
        name to assign to source_file_column
    soc_lookup_table : str
        name to assign to output HIVE soc_lookup_table
    coding_errors_table : str
        name to assign to output HIVE coding_errors_table
    inconsistencies_resolution_table : str
        name of HIVE table containing inconsistencies resolutions
    include_processed : bool, optional
        whether to include files recorded as processed in processed_filenames HIVE table, by default False
    include_invalid : bool, optional
        whether to include files recorded as invalid in error_file_log HIVE table, by default False
    latest_only : bool, optional
        whether to only use the latest file matching the soc_file_pattern, by default False
    start_date : _type_, optional
        before which no file matching the soc_file_pattern will be included, by default None
    end_date : _type_, optional
        after which no file matching the soc_file_pattern will be included, by default None
    """
    join_on_columns = ["work_main_job_title", "work_main_job_role"]
    inconsistencies_resolution_df = extract_from_table(inconsistencies_resolution_table)

    dfs: List[DataFrame] = []
    file_list = get_files_to_be_processed(
        resource_path=soc_file_pattern,
        latest_only=latest_only,
        start_date=start_date,
        end_date=end_date,
        include_processed=include_processed,
        include_invalid=include_invalid,
        date_from_filename=False,
    )

    for file_path in file_list:
        error_message, df = normalise_schema(file_path, soc_schema, soc_regex_map)
        if error_message is None:
            df = assign_filename_column(df, source_file_column)
            dfs.append(df)
        else:
            add_error_file_log_entry(file_path, error_message)  # type: ignore
            print(error_message)  # functional

    if len(dfs) > 0:
        soc_lookup_df = union_multiple_tables(dfs)
        coding_errors_df, soc_lookup_df = transform_cis_soc_data(
            soc_lookup_df, inconsistencies_resolution_df, join_on_columns
        )

        mode = "overwrite" if include_processed else "append"
        update_table_and_log_source_files(soc_lookup_df, soc_lookup_table, source_file_column, "soc_codes", mode)
        update_table(coding_errors_df, coding_errors_table, mode)


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
        """
        Extracts data from csv file to a HIVE table. Parameters control
        which csv file is retrieved from the resource path

        Parameters
        ----------
        resource_path
            path containing one or more csv files
        dataset_name
            _description_, by default dataset_name
        id_column : str, optional
            string specifying unique id column in csv file
        latest_only : bool, optional
            read only the most recent csv file in the resource path
        start_date : date, optional
            filter csv files found in the resource path by those after start_date
        end_date : date, optional
            filter csv files found in the resource path by those before end_date
        include_processed : bool, optional
            read csv files that have already been read
        include_invalid : bool, optional
            read csv files that have not previously matched validation checks
        source_file_column : _type_, optional
            _description_, by default source_file_column
        write_mode : _type_, optional
            _description_, by default write_mode

        Returns
        -------
        dataframe
            saves dataframe to HIVE table
        """
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
            return {"status": "No files"}

        valid_file_paths = validate_files(file_path_list, validation_schema, sep=sep)
        if not valid_file_paths:
            print(f"        - No valid files found in: {resource_path}.")  # functional
            return {"status": "Error"}

        df = extract_validate_transform_input_data(
            include_hadoop_read_write=include_hadoop_read_write,
            resource_path=file_path_list,
            dataset_name=dataset_name,
            id_column=id_column,
            column_name_map=column_name_map,
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
            return {"status": "updated"}
        return df

    _inner_function.__name__ = stage_name
    return _inner_function


@register_pipeline_stage("union_survey_response_files")
def union_survey_response_files(tables_to_process: List, output_survey_table: str):
    """
    Union list of tables_to_process, and write to table.

    Parameters
    ----------
    tables_to_process
        input tables for extracting each of the transformed survey responses tables
    output_survey_table
        output table name for the combine file of all unioned survey responses
    """
    df_list = [extract_from_table(table) for table in tables_to_process]

    df = union_multiple_tables(df_list)
    update_table(df, output_survey_table, "overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("join_lookup_table")
def join_lookup_table(
    input_survey_table: str,
    output_survey_table: str,
    lookup_table_name: str,
    unjoinable_values: Dict[str, Union[str, int]] = {},
    join_on_columns: List[str] = ["work_main_job_title", "work_main_job_role"],
    lookup_transformations: List[str] = [],
    pre_join_transformations: List[str] = [],
    post_join_transformations: List[str] = [],
    output_table_name_key: str = "output_survey_table",
):
    """
    Filters input_survey_table into an unjoinable_df, where all join_on_columns are null, and and applies any
    unjoinable_values to the unjoinable_df.
    Lookup_df is then joined onto the df containing non-null values in join_on_columns
    The df and unjoinable_df are then recombined into an output_survey_table

    Parameters
    ----------
    input_survey_table
    output_survey_table
    lookup_table_name
    unjoinable_values: dict
        dictionary containing {column_name: value_to_assign} pairs to be added to unjoinable_data
    join_on_column: list
        list of columns to join on, defaults to ["work_main_job_title", "work_main_job_role"]
    transformations: list
        list of transformation functions to be run on the dataframe once the lookup has been joined
    """
    transformations_dict = {
        "nims": nims_transformations,
        "blood_past_positive": blood_past_positive_transformations,
        "design_weights_lookup": design_weights_lookup_transformations,
        "replace_design_weights": replace_design_weights_transformations,
        "ordered_household_id": ordered_household_id_tranformations,
    }

    lookup_df = extract_from_table(lookup_table_name)
    for transformation in lookup_transformations:
        lookup_df = transformations_dict[transformation](lookup_df)

    df = extract_from_table(input_survey_table)
    for transformation in pre_join_transformations:
        df = transformations_dict[transformation](df)

    unjoinable_df = df.filter(all_columns_null(join_on_columns))
    for col, val in unjoinable_values.items():
        unjoinable_df = unjoinable_df.withColumn(col, F.lit(val))

    df = df.filter(any_column_not_null(join_on_columns))
    df = left_join_keep_right(df, lookup_df, join_on_columns)
    df = union_multiple_tables([df, unjoinable_df])

    for transformation in post_join_transformations:
        df = transformations_dict[transformation](df)

    update_table(df, output_survey_table, "overwrite")
    return {output_table_name_key: output_survey_table}


@register_pipeline_stage("create_vaccine_regex_lookup")
def create_vaccine_regex_lookup(input_survey_table: str, regex_lookup_table: Optional[str] = None):
    """
    Create or update a regex_lookup_table from an input_survey_table which is filtered
    to include only rows with non-null values in join_on_columns.
    If a regex_lookup_table is a parameter and exists then load the filtered df and get distinct value
    combinations for join_on_columns that are not already in the regex_lookup_table, and update the regex_lookup_table;
    otherwise create regex_lookup_table from distinct value combinations

    Parameters
    ----------
    input_survey_table
    regex_lookup_table
    """
    if regex_lookup_table is not None and check_table_exists(regex_lookup_table):
        lookup_df = extract_from_table(regex_lookup_table, True)
        if not set(lookup_df.columns) == set(["cis_covid_vaccine_type_corrected", "cis_covid_vaccine_type_other_raw"]):
            lookup_df = None
    else:
        lookup_df = None

    df = extract_from_table(input_survey_table)

    for vaccine_number in range(0, 7):
        vaccine_type_col = (
            "cis_covid_vaccine_type_other" if vaccine_number == 0 else f"cis_covid_vaccine_type_other_{vaccine_number}"
        )

        df = df.filter(F.col(vaccine_type_col).isNotNull())

        if lookup_df is not None:
            renamed_lookup_df = lookup_df.withColumnRenamed("cis_covid_vaccine_type_other_raw", vaccine_type_col)

            non_derived_rows = df.join(renamed_lookup_df, on=vaccine_type_col, how="leftanti")
            non_derived_rows = non_derived_rows.dropDuplicates([vaccine_type_col])
            count = non_derived_rows.count()
            print(f"     - located regex lookup df with {count} additional rows to process")  # functional
            if count > 0:
                lookup_df = lookup_df.union(process_vaccine_regex(non_derived_rows, vaccine_type_col))

        else:
            df_to_process = df.dropDuplicates([vaccine_type_col])
            print(
                f"     - creating regex lookup table from {df_to_process.count()} rows. This may take some time ... "
            )  # functional
            partitions = int(get_or_create_spark_session().sparkContext.getConf().get("spark.sql.shuffle.partitions"))
            partitions = int(partitions / 2)
            df_to_process = df_to_process.repartition(partitions)
            lookup_df = process_vaccine_regex(df_to_process, vaccine_type_col)
        update_table(lookup_df, regex_lookup_table, "overwrite")


@register_pipeline_stage("join_vaccine_lookup")
def update_vaccine_types(input_survey_table: str, output_survey_table: str, vaccine_type_lookup: str):
    df = extract_from_table(input_survey_table)
    lookup_df = extract_from_table(vaccine_type_lookup)
    for vaccine_number in range(0, 7):

        vaccine_type_other_col = "cis_covid_vaccine_type_other"
        vaccine_type_col = "cis_covid_vaccine_type"
        vaccine_date_col = "cis_covid_vaccine_date"

        if vaccine_number > 0:
            vaccine_type_other_col = f"{vaccine_type_other_col}_{vaccine_number}"
            vaccine_type_col = f"{vaccine_type_col}_{vaccine_number}"
            vaccine_date_col = f"{vaccine_date_col}_{vaccine_number}"

        # match the raw column to the vaccine number
        renamed_lookup = lookup_df.withColumnRenamed("cis_covid_vaccine_type_other_raw", vaccine_type_other_col)
        # join on 'vaccine_type_other_col' such that the cleaned data can be applied
        df = df.join(renamed_lookup, on=vaccine_type_other_col, how="left")

        # remove date dependent vaccine type if after date
        df = df.withColumn(
            "cis_covid_vaccine_type_corrected",
            F.when(
                F.col(vaccine_date_col) >= "2021-01-31",
                F.expr("filter(cis_covid_vaccine_type_corrected, x -> x != 'Pfizer/BioNTechDD')"),
            ).otherwise(F.col("cis_covid_vaccine_type_corrected")),
        )
        # get first valid type
        df = df.withColumn("cis_covid_vaccine_type_corrected", F.col("cis_covid_vaccine_type_corrected").getItem(0))

        # update vaccine type column
        # set vaccine type col to none when default value
        df = df.withColumn(
            vaccine_type_col,
            F.when(F.col(vaccine_type_col) == "Other / specify", F.col("cis_covid_vaccine_type_corrected")).otherwise(
                F.col(vaccine_type_col)
            ),
        ).drop("cis_covid_vaccine_type_corrected")

        df = df.withColumn(
            vaccine_type_col,
            F.when(F.col(vaccine_type_col) == "Pfizer/BioNTechDD", "Pfizer/BioNTech").otherwise(
                F.col(vaccine_type_col)
            ),
        )
        df = df.withColumn(vaccine_type_other_col, F.lit(None).cast("string"))
        df = df.withColumn(
            vaccine_type_col,
            F.when(F.col(vaccine_type_col).isNull(), "Don't know type").otherwise(F.col(vaccine_type_col)),
        )
    update_table(df, output_survey_table, "overwrite")


@register_pipeline_stage("create_healthcare_regex_lookup")
def create_regex_lookup(input_survey_table: str, regex_lookup_table: Optional[str] = None):
    """
    Create or update a regex_lookup_table from an input_survey_table which is filtered
    to include only rows with non-null values in join_on_columns.
    If a regex_lookup_table is a parameter and exists then load the filtered df and get distinct value
    combinations for join_on_columns that are not already in the regex_lookup_table, and update the regex_lookup_table;
    otherwise create regex_lookup_table from distinct value combinations

    Parameters
    ----------
    input_survey_table
    regex_lookup_table
    """
    join_on_columns = ["work_main_job_title", "work_main_job_role"]
    df = extract_from_table(input_survey_table)
    df = df.filter(any_column_not_null(join_on_columns))
    if regex_lookup_table is not None and check_table_exists(regex_lookup_table):
        lookup_df = extract_from_table(regex_lookup_table, True)
        non_derived_rows = df.join(lookup_df, on=join_on_columns, how="leftanti")
        non_derived_rows = non_derived_rows.dropDuplicates(join_on_columns)
        print(
            f"     - located regex lookup df with {non_derived_rows.count()} additional rows to process"
        )  # functional
        update_table(process_healthcare_regex(non_derived_rows), regex_lookup_table, "append")
    else:
        df_to_process = df.dropDuplicates(join_on_columns)
        print(
            f"     - creating regex lookup table from {df_to_process.count()} rows. This may take some time ... "
        )  # functional
        partitions = int(get_or_create_spark_session().sparkContext.getConf().get("spark.sql.shuffle.partitions"))
        partitions = int(partitions / 2)
        df_to_process = df_to_process.repartition(partitions)
        lookup_df = process_healthcare_regex(df_to_process)
        update_table(lookup_df, regex_lookup_table, "overwrite")


@register_pipeline_stage("join_blood_positive_lookup")
def join_blood_positive_lookup(lookup_table_name: str, input_survey_table: str, output_survey_table: str):
    """
    Stage to join blood positive lookup table
    """
    blood_positive_lookup_df = extract_from_table(lookup_table_name)
    df = extract_from_table(input_survey_table)
    df = df.join(
        blood_positive_lookup_df,
        on="ons_household_id",
        how="left",
    )
    df = df.withColumn("blood_past_positive_flag", F.when(F.col("blood_past_positive").isNull(), 0).otherwise(1))
    update_table(df, output_survey_table, "overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("lookup_based_editing")
def lookup_based_editing(
    input_survey_table: str,
    cohort_lookup_table: str,
    travel_countries_lookup_table: str,
    tenure_group_table: str,
    output_survey_table: str,
):
    """
    Edit columns based on mappings from lookup tables. Often used to correct data quality issues.
    Requires lookup_tables to exist or be created prior to being called

    Parameters
    ----------
    input_survey_table
    cohort_lookup_table
        input table name for cohort corrections lookup table
    travel_countries_lookup_table
        input table name for travel_countries corrections lookup table
    tenure_group_table
        input table name for tenure_group corrections lookup table
    output_survey_table
    """

    df = extract_from_table(input_survey_table)
    cohort_lookup = extract_from_table(cohort_lookup_table)
    travel_countries_lookup = extract_from_table(travel_countries_lookup_table)
    tenure_group = extract_from_table(tenure_group_table).select(
        "UAC", "numAdult", "numChild", "dvhsize", "tenure_group"
    )
    for lookup_table_name, lookup_df, join_on_column_list in zip(
        [cohort_lookup_table, travel_countries_lookup_table, tenure_group_table],
        [cohort_lookup, travel_countries_lookup, tenure_group],
        [["participant_id", "old_cohort"], ["been_outside_uk_last_country_old"], ["UAC"]],
    ):
        check_lookup_table_joined_columns_unique(
            df=lookup_df, join_column_list=join_on_column_list, name_of_df=lookup_table_name
        )

    df = transform_from_lookups(df, cohort_lookup, travel_countries_lookup, tenure_group)
    update_table(df, output_survey_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("union_dependent_transformations")
def execute_union_dependent_transformations(input_survey_table: str, output_survey_table: str):
    """
    Transformations that require the union of the different input survey response files.
    Includes filling forwards or backwards over time and deriving new information over time.

    Parameters
    ----------
    input_survey_table
    output_survey_table
    """
    df = extract_from_table(input_survey_table)
    df = fill_forwards_transformations(df)
    df = union_dependent_cleaning(df)
    df = union_dependent_derivations(df)
    update_table(df, output_survey_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("fill_forwards_events")
def execute_fill_forwards_events(input_survey_table: str, output_survey_table: str):
    """
    Separates out the fill_forwards_event implementation of last observation carried forwards (LOCF) logic from STATA code

    Parameters
    ----------
    input_survey_table
    output_survey_table
    """
    df = extract_from_table(input_survey_table)
    df = fill_forward_events_for_key_columns(df)
    update_table(df, output_survey_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("join_geographic_data")
def join_geographic_data(
    geographic_table: str,
    input_survey_table: str,
    output_survey_table: str,
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
    survey_responses_df = extract_from_table(input_survey_table)
    geographic_survey_df = survey_responses_df.drop("postcode", "region_code").join(
        design_weights_df, on=id_column, how="left"
    )
    update_table(geographic_survey_df, output_survey_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("replace_design_weights")
def replace_design_weights(
    design_weight_lookup_table: str,
    input_survey_table: str,
    output_survey_table: str,
    design_weight_columns: List[str],
):
    """
    Temporary stage to replace design weights by lookup.
    Also makes temporary edits to fix raw data issues in geographies.

    Parameters
    ----------
    design_weight_lookup_table
    input_survey_table
    output_survey_table
    design_weight_columns: list
        list of columns to be replaced with values from design_weight_lookup_table

    """
    design_weight_lookup = extract_from_table(design_weight_lookup_table)
    df = extract_from_table(input_survey_table)
    df = df.drop(*design_weight_columns)
    df = df.join(
        design_weight_lookup.select(*design_weight_columns, "ons_household_id"), on="ons_household_id", how="left"
    )

    df = df.withColumn(
        "local_authority_unity_authority_code",
        F.when(F.col("local_authority_unity_authority_code") == "E06000062", "E07000154")
        .when(F.col("local_authority_unity_authority_code") == "E06000061", "E07000156")
        .otherwise(F.col("local_authority_unity_authority_code")),
    )
    df = df.withColumn(
        "region_code",
        F.when(F.col("region_code") == "W92000004", "W99999999")
        .when(F.col("region_code") == "S92000003", "S99999999")
        .when(F.col("region_code") == "N92000002", "N99999999")
        .otherwise(F.col("region_code")),
    )

    update_table(df, output_survey_table, "overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("impute_demographic_columns")
def impute_demographic_columns(input_survey_table: str, imputed_values_table: str, output_survey_table: str):
    """
    Impute values for sex, ethnicity and date of birth.
    Assumes that columns to be imputed have been filled forwards, as the latest value from each participant is used.
    Specific imputations are carried out for for each key demographic column. The resulting columns should have no
    missing values.
    Stores imputed values in a lookup table, for reuse in subsequent imputation rounds. This table is also backed up
    with a datetime suffix.
    Also outputs a table of survey response records with imputed values.
    Note that this stage depends on geography information from the sample files being available
    (from sample file processing).

    Parameters
    ----------
    input_survey_table
    imputed_values_table
        name of HIVE table containing previously imputed values by participant
    output_survey_table
    """
    imputed_value_lookup_df = None
    if check_table_exists(imputed_values_table):
        imputed_value_lookup_df = extract_from_table(imputed_values_table, break_lineage=True)
    df = extract_from_table(input_survey_table)

    key_columns_imputed_df = impute_key_columns(
        df, imputed_value_lookup_df, get_config().get("imputation_log_directory", "./")
    )
    df_with_imputed_values, new_imputed_value_lookup = post_imputation_wrapper(df, key_columns_imputed_df)

    update_table(new_imputed_value_lookup, imputed_values_table, "overwrite", archive=True)
    update_table(df_with_imputed_values, output_survey_table, "overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("geography_and_imputation_dependent_logic")
def geography_and_imputation_dependent_processing(
    input_survey_table: str,
    rural_urban_lookup_path: str,
    output_survey_table: str,
):
    """
    Processing that depends on geographies and and imputed demographic infromation.

    Parameters
    ----------
    input_survey_table
        name of the table containing data to be processed
    rural_urban_lookup_path
        path to the rural urban lookup to be joined onto responses
    edited_table
        name of table to write processed data to
    """
    df = extract_from_table(input_survey_table)
    rural_urban_lookup_df = get_or_create_spark_session().read.csv(
        rural_urban_lookup_path,
        header=True,
        schema="""
            lower_super_output_area_code_11 string,
            cis_rural_urban_classification string
        """,
    )  # Prefer version from sample
    df = df.join(
        F.broadcast(rural_urban_lookup_df),
        how="left",
        on="lower_super_output_area_code_11",
    )
    df = assign_outward_postcode(df, "outward_postcode", reference_column="postcode")

    df = assign_multigenerational(
        df=df,
        column_name_to_assign="multigenerational_household",
        participant_id_column="participant_id",
        household_id_column="ons_household_id",
        visit_date_column="visit_datetime",
        date_of_birth_column="date_of_birth",
        country_column="country_name_12",
    )  # Includes school year and age_at_visit derivations

    df = derive_age_based_columns(df, "age_at_visit")
    df = assign_age_group_school_year(
        df,
        country_column="country_name_12",
        age_column="age_at_visit",
        school_year_column="school_year",
        column_name_to_assign="age_group_school_year",
    )
    df = assign_work_patient_facing_now(
        df,
        column_name_to_assign="work_patient_facing_now",
        age_column="age_at_visit",
        work_healthcare_column="work_health_care_patient_facing",
    )
    df = assign_work_person_facing_now(
        df,
        column_name_to_assign="work_person_facing_now",
        work_patient_facing_now_column="work_patient_facing_now",
        work_social_care_column="work_social_care",
        age_at_visit_column="age_at_visit",
    )

    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status_v0",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    df = reclassify_work_variables(df, spark_session=get_or_create_spark_session(), drop_original_variables=False)
    df = fill_forward_only_to_nulls(
        df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "work_status_v0",
            "work_status_v1",
            "work_status_v2",
            "work_location",
            "work_not_from_home_days_per_week",
        ],
    )
    df = create_formatted_datetime_string_columns(df)
    update_table(df, output_survey_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("validate_survey_responses")
def validate_survey_responses(
    input_survey_table: str,
    duplicate_count_column_name: str,
    validation_failure_flag_column: str,
    output_survey_table: str,
    invalid_survey_responses_table: str,
    valid_validation_failures_table: str,
    invalid_validation_failures_table: str,
    id_column: str,
):
    """
    Populate error column with outcomes of specific validation checks against fully
    transformed survey dataset.

    Parameters
    ----------
    input_survey_table
    duplicate_count_column_name
        column name in which to count duplicates of rows within the dataframe
    validation_failure_flag_column
        name for error column wherein each of the validation checks results are appended
    output_survey_table
        table containing results that passed the error checking process
    invalid_survey_responses_table
        table containing results that failed the error checking process
    valid_validation_failures_table
        table containing valid failures from the error checking process
    invalid_validation_failures_table
        table containing invalid failures from the error checking process
    id_column
        string specifying id column in input_survey_table
    """
    unioned_survey_responses = extract_from_table(input_survey_table)
    valid_survey_responses, erroneous_survey_responses = validation_ETL(
        df=unioned_survey_responses,
        validation_check_failure_column_name=validation_failure_flag_column,
        duplicate_count_column_name=duplicate_count_column_name,
    )

    validation_check_failures_valid_data_df = (
        (
            valid_survey_responses.select(id_column, validation_failure_flag_column).withColumn(
                "validation_check_failures", F.explode(validation_failure_flag_column)
            )
        )
        .withColumn("run_id", F.lit(get_run_id()))
        .drop(validation_failure_flag_column)
    )

    validation_check_failures_invalid_data_df = (
        (
            erroneous_survey_responses.select(id_column, validation_failure_flag_column).withColumn(
                "validation_check_failures", F.explode(validation_failure_flag_column)
            )
        )
        .withColumn("run_id", F.lit(get_run_id()))
        .drop(validation_failure_flag_column)
    )
    # valid_survey_responses = fix_timestamps(valid_survey_responses)
    # invalid_survey_responses_table = fix_timestamps(erroneous_survey_responses)
    update_table(validation_check_failures_valid_data_df, valid_validation_failures_table, write_mode="append")
    update_table(validation_check_failures_invalid_data_df, invalid_validation_failures_table, write_mode="append")
    update_table(valid_survey_responses, output_survey_table, write_mode="overwrite", archive=True)
    update_table(erroneous_survey_responses, invalid_survey_responses_table, write_mode="overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("report")
def report(
    unique_id_column: str,
    validation_failure_flag_column: str,
    duplicate_count_column_name: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
    valid_survey_responses_errors_table: str,
    invalid_survey_responses_errors_table: str,
    output_directory: str,
    tables_to_count: List[str],
    error_priority_map: dict = {},
):
    """
    Create a excel spreadsheet with multiple sheets to summarise key data from various
    tables regarding the running of the pipeline; using overall and most recent statistics.

    Parameters
    ----------
    unique_id_column
        column that should hold unique id for each row in responses file
    validation_failure_flag_column
        name of the column containing the previously created to contain validation error messages
        name should match that created in validate_survey_responses stage
    duplicate_count_column_name
        name of the column containing the previously created to contain count of rows that repeat
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

    valid_df_errors = generate_error_table(valid_survey_responses_errors_table, error_priority_map)
    invalid_df_errors = generate_error_table(invalid_survey_responses_errors_table, error_priority_map)
    soc_uncode_count = count_variable_option(valid_df, "standard_occupational_classification_code", "uncodeable")
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

    select_cols = [
        "visit_id",
        "visit_datetime",
        *[col for col in valid_df.columns if col.startswith("cis_covid_vaccine_type")],
    ]
    other_vaccine_df = valid_df.filter(F.col("cis_covid_vaccine_type") == "Don't know type").select(*select_cols)

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

        other_vaccine_df.toPandas().to_excel(writer, sheet_name="un-coded vaccines", index=False)
        counts_df.to_excel(writer, sheet_name="dataset totals", index=False)
        valid_df_errors.toPandas().to_excel(writer, sheet_name="validation fails valid data", index=False)
        invalid_df_errors.toPandas().to_excel(writer, sheet_name="validation fails invalid data", index=False)
        duplicated_df.toPandas().to_excel(writer, sheet_name="duplicated record summary", index=False)
        soc_uncode_count.toPandas().to_excel(writer, sheet_name="'uncodeable' soc code count", index=False)

    write_string_to_file(
        output.getbuffer(), f"{output_directory}/report_output_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    )


@register_pipeline_stage("lab_report")
def lab_report(input_survey_table: str, swab_report_table: str, blood_report_table: str) -> DataFrame:
    """Generate reports of most recent 7 days of swab and blood data"""
    survey_responses_df = extract_from_table(input_survey_table).orderBy("file_date")
    swab_df, blood_df = generate_lab_report(survey_responses_df)
    update_table(swab_df, swab_report_table, "overwrite")
    update_table(blood_df, blood_report_table, "overwrite")


@register_pipeline_stage("tables_to_csv")
def tables_to_csv(
    outgoing_directory,
    tables_to_csv_config_file,
    category_map,
    filter={},
    sep="|",
    extension=".txt",
    dry_run=False,
    accept_missing=True,
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
    filter
        a dictionary of column to value list maps that where the row cell must contain a value in given list to be output to CSV
    dry_run
        when set to True, will delete files after they are written (for testing). Default is False.
    accept_missing
        remove missing columns from map if not in dataframe
    """
    output_datetime = datetime.today()
    output_datetime_str = output_datetime.strftime("%Y%m%d_%H%M%S")

    file_directory = Path(outgoing_directory) / output_datetime_str
    manifest = Manifest(outgoing_directory, pipeline_run_datetime=output_datetime, dry_run=dry_run)
    category_map_dictionary = category_maps.get(category_map)

    config_file = get_secondary_config(tables_to_csv_config_file)

    for table in config_file["create_tables"]:
        df = extract_from_table(table["table_name"])
        if table["column_name_map"] is not None:
            if accept_missing:
                columns_to_select = [element for element in table["column_name_map"].keys() if element in df.columns]
            else:
                columns_to_select = [element for element in table["column_name_map"].keys()]
                missing_columns = set(columns_to_select) - set(df.columns)
                if missing_columns:
                    raise ValueError(f"Columns missing in {table['table_name']}: {missing_columns}")

            df = df.select(*columns_to_select)

        if len(filter.keys()) > 0:
            filter = {key: val if type(val) == list else [val] for key, val in filter.items()}
            df = df.filter(reduce(and_, [F.col(col).isin(val) for col, val in filter.items()]))

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


@register_pipeline_stage("compare_tables")
def compare(
    base_table_name: str,
    table_name_to_compare: str,
    counts_df_table_name: str,
    diff_samples_table_name: str,
    unique_id_column: str = "unique_participant_response_id",
    num_samples: int = 10,
    select_columns: List[str] = [],  # type: ignore
):
    """
    Create an output that holds information about differences between 2 tables

    Parameters
    ----------
    base_table_name
    table_name_to_compare
    counts_df_table_name
    diff_samples_table_name
    unique_id_column
        column containing unique id common to base an compare dataframes
    num_samples
        number of examples of each differing row to provide in the output table
    select_columns
        optional subset of columns to evaluate
    """
    base_df = extract_from_table(base_table_name)
    base_df = assign_unique_id_column(
        base_df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id"]
    )
    compare_df = extract_from_table(table_name_to_compare)
    compare_df = assign_unique_id_column(
        compare_df, "unique_participant_response_id", concat_columns=["visit_id", "participant_id"]
    )
    if len(select_columns) > 0:
        if unique_id_column not in select_columns:
            select_columns = [unique_id_column, *select_columns]
            compare_df = compare_df.select(*select_columns)
            base_df = base_df.select(*select_columns)
    counts_df, difference_sample_df = get_differences(base_df, compare_df, unique_id_column, num_samples)
    total = counts_df.select(F.sum(F.col("difference_count"))).collect()[0][0]
    print(f"     {table_name_to_compare} contained {total} differences to {base_table_name}")  # functional
    update_table(counts_df, counts_df_table_name, "overwrite")
    update_table(difference_sample_df, diff_samples_table_name, "overwrite")


@register_pipeline_stage("check_predictions")
def check_predictions(
    base_table_name: str,
    table_name_to_compare: str,
    prediction_results_table: str,
    unique_id_column: str = "unique_participant_response_id",
):
    """
    Create an output that holds information about differences between 2 tables

    Parameters
    ----------
    base_table_name
    table_name_to_compare
    unique_id_column
        column containing unique id common to base an compare dataframes
    """
    base_df = extract_from_table(base_table_name)
    compare_df = extract_from_table(table_name_to_compare)
    pc = PredictionChecker(base_df, compare_df, unique_id_column)
    df = pc.check_predictions()
    update_table(df, prediction_results_table, "overwrite")


@register_pipeline_stage("generate_sample")
def sample_df(
    table_name, sample_type, cols, cols_to_evaluate, rows_per_file, num_files, output_folder_name, filter_condition=None
):
    df = extract_from_table(table_name)
    if filter_condition is not None:
        df = df.filter(eval(filter_condition))
    generate_sample(df, sample_type, cols, cols_to_evaluate, rows_per_file, num_files, output_folder_name)


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


@register_pipeline_stage("calculate_household_level_populations")
def calculate_household_level_populations(
    address_lookup_table,
    postcode_lookup_table,
    lsoa_cis_lookup_table,
    country_lookup_table,
    household_level_populations_table,
):
    """
    Calculate counts of households by CIS area 20 and country code 12 geographical groups used in the design weight
    calculation.
    Combines several lookup tables to get the necessary geographies linked to households, then sums households by
    CIS area and country code.

    Parameters
    ----------
    address_lookup_table
        addressbase HIVE table name
    postcode_lookup_table
        NSPL postcode lookup HIVE table name to join onto addressbase to get LSOA 11 and country code 12
    lsoa_cis_lookup_table
        LSOA 11 to CIS lookup HIVE table name to get CIS area codes
    country_lookup_table
        country lookup HIVE table name to get country names from country code 12
    household_level_populations_table
        HIVE table to write household level populations to
    """
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


@register_pipeline_stage("record_level_interface")
def record_level_interface(
    input_survey_table: str,
    csv_editing_file: str,
    unique_id_column: str,
    unique_id_list: List,
    output_survey_table: str,
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
            - id_column_name (optional)
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
    df = extract_from_table(input_survey_table)

    filtered_out_df = df.filter(F.col(unique_id_column).isin(unique_id_list))
    update_table(filtered_out_df, filtered_survey_responses_table, "overwrite")

    lookup_df = extract_lookup_csv(csv_editing_file, validation_schemas["csv_lookup_schema"])
    filtered_in_df = df.filter(~F.col(unique_id_column).isin(unique_id_list))
    edited_df = update_from_lookup_df(filtered_in_df, lookup_df, id_column=unique_id_column)
    update_table(edited_df, output_survey_table, "overwrite")
    return {"output_survey_table": output_survey_table}


@register_pipeline_stage("sample_file_ETL")
def sample_file_ETL(
    household_level_populations_table: str,
    old_sample_file: str,
    new_sample_file: str,
    new_sample_source_name: str,
    postcode_lookup: str,
    master_sample_file: str,
    design_weight_table: str,
    country_lookup: str,
    lsoa_cis_lookup: str,
    tranche_file_path: Optional[str] = None,
    tranche_strata_columns: Optional[List[str]] = None,
):
    """
    Process a new sample file, to union it with previous sample data and calculate new swab and antibody design weights.
    Creates a table of geographies and design weights per household.

    Carries out different scenarios for antibody design weight for either:
    1. Where no tranche is provided, or no new households have been sampled
    2. Where a tranche has been provided and new households have been sampled

    ``old_sample_file`` may point to the same table as ``design_weight_table``, to reuse the values from the previous
    sample file processing run.

    To process a new sample file, the following *must* be updated:
    - ``new_sample_file``
    - ``new_sample_source_name``
    - ``tranche_file_path``

    The output ``design_weight_table`` is also stored as a backup table with the current datetime.

    Notes
    -----
    Lookup tables are referenced here are used to get data that are missing on the master sample and sample
    files, which are required to link on postcode. Once these issues are resolved, this part of the code may be
    simplified to include only:
    - household_level_populations_table
    - old_sample_file
    - new_sample_file
    - new_sample_source_name
    - tranche_file_path
    - tranche_strata_columns

    This is dependent on receiving the new sample file in the format expected as specified in the excel specification.

    Paramaters
    ----------
    household_level_populations_table
        HIVE table create by household level population calculation, containing population by CIS area and country
    old_sample_file
        HIVE table containing the previously processed sample files, including design weights
    new_sample_file
        CSV file or HIVE table containing the new sample to be processed
    new_sample_source_name
        string constant to be stored as the sample source name for the new sample records
    postcode_lookup
        HIVE table containing the NSPL postcode lookup
    master_sample_file
        HIVE table containing the master sample
    design_weight_table
        HIVE table to write household geographies and design weights to
    country_lookup
        HIVE table containing country code 12 to country name lookup
    lsoa_cis_lookup
        HIVE table containing LSOA 11 to CIS area 20 lookup
    tranche_file_path
        path to tranche CSV file, if a tranche is required for the current sample file, otherwise leave empty in config
    tranche_strata_columns
        list of column names to be used as strata in tranche factor calculations
    """
    first_run = not check_table_exists(design_weight_table)

    postcode_lookup_df = extract_from_table(postcode_lookup)
    lsoa_cis_lookup_df = extract_from_table(lsoa_cis_lookup)
    country_lookup_df = extract_from_table(country_lookup)
    old_sample_df = extract_from_table(old_sample_file, break_lineage=True)
    master_sample_df = extract_from_table(master_sample_file)

    new_sample_df = extract_lookup_csv(
        new_sample_file,
        validation_schemas["new_sample_file_schema"],
        column_name_maps["new_sample_file_column_map"],
        True,
    )
    tranche_df = None
    if tranche_file_path is not None:
        if tranche_strata_columns is None:
            raise ValueError("`tranche_strata_columns` must be provided when a `tranche_file_path` has been provided")
        tranche_df = extract_lookup_csv(
            tranche_file_path, validation_schemas["tranche_schema"], column_name_maps["tranche_column_map"], True
        )

    household_level_populations_df = extract_from_table(household_level_populations_table)
    design_weights = calculate_design_weights(
        household_level_populations_df,
        master_sample_df,
        old_sample_df,
        new_sample_df,
        new_sample_source_name,
        tranche_df,
        postcode_lookup_df,
        country_lookup_df,
        lsoa_cis_lookup_df,
        first_run,
        tranche_strata_columns,
    )
    update_table(design_weights, design_weight_table, write_mode="overwrite", archive=True)


@register_pipeline_stage("aggregated_output")
def aggregated_output(
    apply_aggregate_type,
    input_survey_table_to_aggregate,
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
    df = extract_from_table(table_name=input_survey_table_to_aggregate)

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
        table_name=f"{input_survey_table_to_aggregate}_{apply_aggregate_type}",
        write_mode="overwrite",
    )
