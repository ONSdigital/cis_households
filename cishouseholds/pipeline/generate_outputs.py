import subprocess
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import List
from typing import Optional
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.edit import assign_from_map
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_from_csv_lookup
from cishouseholds.extract import list_contents
from cishouseholds.hdfs_utils import read_header
from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.pipeline.category_map import category_maps
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.manifest import Manifest
from cishouseholds.pipeline.output_variable_name_map import output_name_map
from cishouseholds.pipeline.output_variable_name_map import update_output_name_maps
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("report")
def report(
    unique_id_column: str,
    validation_failure_flag_column: str,
    duplicate_row_flag_column: str,
    valid_survey_responses_table: str,
    invalid_survey_responses_table: str,
    filtered_survey_responses_table: str,
    output_directory: str,
):
    valid_df = extract_from_table(valid_survey_responses_table)
    invalid_df = extract_from_table(invalid_survey_responses_table)
    filtered_df = extract_from_table(filtered_survey_responses_table)
    # processed_file_log = extract_from_table("processed_filenames")
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

    duplicated_df = valid_df.select(unique_id_column, duplicate_row_flag_column).filter(
        F.col(duplicate_row_flag_column) > 1
    )

    counts_df = pd.DataFrame(
        {
            "dataset": [
                "invalid input files",
                "valid survey responses",
                "invalid survey responses",
                "filtered survey responses",
            ],
            "count": [
                invalid_files_count,
                valid_survey_responses_count,
                filtered_survey_responses_count,
                invalid_survey_responses_count,
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
    input_df = extract_from_table(survey_responses_table)
    edited_df = update_from_csv_lookup(df=input_df, csv_filepath=csv_editing_file, id_column=unique_id_column)
    update_table(edited_df, edited_survey_responses_table, "overwrite")

    filtered_df = edited_df.filter(F.col(unique_id_column).isin(unique_id_list))
    update_table(filtered_df, filtered_survey_responses_table, "overwrite")


@register_pipeline_stage("tables_to_csv")
def tables_to_csv(
    table_file_pairs, outgoing_directory, update_name_map=None, category_map="default_category_map", dry_run=False
):
    """
    Writes data from an existing HIVE table to csv output, including mapping of column names and values.

    Takes a list of 2-item tuples or lists:
        table_file_pairs:
            - [HIVE_table_name, output_csv_file_name]

    Optionally also point to an update map to be used for the variable name mapping of these outputs.
    """
    output_datetime = datetime.today()
    output_datetime_str = output_datetime.strftime("%Y%m%d_%H%M%S")

    file_directory = Path(outgoing_directory) / output_datetime_str
    manifest = Manifest(outgoing_directory, pipeline_run_datetime=output_datetime, dry_run=dry_run)

    name_map_dictionary = output_name_map.copy()
    if update_name_map is not None:
        name_map_dictionary.update(update_output_name_maps[update_name_map])
    category_map_dictionary = category_maps[category_map]

    for table_name, output_file_name in table_file_pairs:
        df = extract_from_table(table_name)
        df = map_output_values_and_column_names(df, name_map_dictionary, category_map_dictionary)

        file_path = file_directory / f"{output_file_name}_{output_datetime_str}"
        write_csv_rename(df, file_path)
        file_path = file_path.with_suffix(".csv")
        header_string = read_header(file_path)
        manifest.add_file(
            relative_file_path=file_path.relative_to(outgoing_directory).as_posix(),
            column_header=header_string,
            validate_col_name_length=False,
        )

    manifest.write_manifest()


@register_pipeline_stage("generate_outputs")
def generate_outputs(output_directory, merged_antibody_swab_table):
    output_datetime = datetime.today().strftime("%Y%m%d-%H%M%S")
    output_directory = Path(output_directory) / output_datetime
    # TODO: Check that output dir exists

    linked_df = extract_from_table(merged_antibody_swab_table)

    #    all_visits_df = extract_from_table("response_level_records")
    #    participant_df = extract_from_table("participant_level_with_vaccination_data")

    #    linked_df = all_visits_df.join(participant_df, on="participant_id", how="left")
    linked_df = linked_df.withColumn(
        "completed_visits_subset",
        F.when(
            (F.col("participant_visit_status") == "Completed")
            | (F.col("blood_sample_barcode").isNotNull() | (F.col("swab_sample_barcode").isNotNull())),
            True,
        ).otherwise(False),
    )

    all_visits_output_df = map_output_values_and_column_names(
        linked_df, output_name_map, category_maps["default_category_map"]
    )

    complete_visits_output_df = all_visits_output_df.where(F.col("completed_visits_subset"))

    write_csv_rename(
        all_visits_output_df.drop("completed_visits_subset"),
        output_directory / f"cishouseholds_all_visits_{output_datetime}",
    )
    write_csv_rename(
        complete_visits_output_df.drop("completed_visits_subset"),
        output_directory / f"cishouseholds_completed_visits_{output_datetime}",
    )


def map_output_values_and_column_names(df: DataFrame, column_name_map: dict, value_map_by_column: dict):
    """
    Map column values to numeric categories and rename columns based on maps.
    Only applies when column exists in map and df.
    """
    value_map_by_column = {k: v for k, v in value_map_by_column.items() if k in df.columns}
    column_name_map = {column: column_name_map.get(column, column) for column in df.columns}
    for column, value_map in value_map_by_column.items():
        df = assign_from_map(df, column, column, value_map)
    df = rename_column_names(df, column_name_map)
    return df


def check_columns(col_args, selection_columns, error):
    arguments = ["group by columns ", "name map", "value map"]
    for argument, check in zip(arguments, col_args):  # type: ignore
        if check is not None:
            for column in check:  # type: ignore
                if column not in selection_columns:  # type: ignore
                    if error == 1:
                        raise IndexError(
                            f"column:{column} is required for {argument}, therefore they must be selected in arguments"
                        )
                    else:
                        raise AttributeError(f"column: {column} does not exist in dataframe")


def configure_outputs(
    df: DataFrame,
    selection_columns: Optional[Union[List[str], str]] = None,
    group_by_columns: Optional[Union[List[str], str]] = None,
    aggregate_function: Optional[Any] = None,
    aggregate_column_name: Optional[str] = None,
    name_map: Optional[dict] = None,
    value_map: Optional[dict] = None,
    complete_map: Optional[bool] = False,
):
    """
    Customise the output of the pipeline using user inputs
    Parameters
    ----------
    df
    selection_columns
    group_by_columns
    name_map
        dictionary containy key value pairs of old and new column names to modify
    value_map
        dicitonary with key value pair: {column: mapping expression dictionary} to map values in given columns
    complete_map
        boolean expression to return error if all values in column must be mapped to constitue a correct output
    """
    col_args = []
    if type(group_by_columns) != list and group_by_columns is not None:
        group_by_columns = [str(group_by_columns)]
    if type(selection_columns) != list and selection_columns is not None:
        selection_columns = [str(selection_columns)]
    if group_by_columns is not None:
        col_args.append(group_by_columns)
    if value_map is not None:
        col_args.append(value_map.keys())  # type: ignore
    if name_map is not None:
        col_args.append(name_map.keys())  # type: ignore
    if selection_columns is not None:
        check_columns(col_args, selection_columns, 1)

    check_columns([*col_args, selection_columns], df.columns, 0)

    if group_by_columns is not None:
        if aggregate_function is None:
            raise Exception("Aggregate function required: rows can only be grouped using an aggregation function")
        if aggregate_column_name is not None:
            prev_cols = set(df.columns)
            df = df.groupBy(*group_by_columns).agg({"*": aggregate_function})
            new_col = list(set(df.columns) - prev_cols)[0]
            df = df.withColumnRenamed(new_col, aggregate_column_name)
        else:
            df = df.groupBy(*group_by_columns).agg({"*": aggregate_function})
    if name_map is not None:
        for current_name, to_be_name in name_map.items():
            df = df.withColumnRenamed(current_name, to_be_name)
    if value_map is not None:
        for column_name_to_assign, map in value_map.items():
            df = update_column_values_from_map(
                df=df, column=column_name_to_assign, map=map, error_if_value_not_found=complete_map
            )
    return df


def write_csv_rename(df: DataFrame, file_path: Path):
    """
    Writes a df to file_path as a single partition and moves to a single CSV with the same name.

    Process first writes into outfile/_tmp and then copies the file
    to rename it.

    Parameters
    ----------
    df
    file_path
        path to outgoing file, without filename extension
    """
    temp_path = file_path / "_tmp"
    (df.coalesce(1).write.mode("overwrite").csv(temp_path.as_posix(), header=True))

    partitions = list_contents(temp_path.as_posix())["filename"].dropna().tolist()

    partitions = [part for part in partitions if part.endswith(".csv")]

    # move temp file to target location and rename
    subprocess.check_call(["hadoop", "fs", "-mv", (temp_path / partitions[0]), file_path.as_posix() + ".csv"])

    # remove original subfolder inc tmp
    subprocess.call(["hadoop", "fs", "-rm", "-r", file_path], stdout=subprocess.DEVNULL)
