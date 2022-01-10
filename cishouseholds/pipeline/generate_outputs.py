import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import List
from typing import Optional
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.edit import assign_from_map
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.extract import list_contents
from cishouseholds.pipeline.category_map import category_map
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.output_variable_name_map import output_name_map
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("generate_outputs")
def generate_outputs():
    config = get_config()
    output_datetime = datetime.today().strftime("%Y%m%d%H%M%S")
    output_directory = Path(config["output_directory"]) / output_datetime
    # TODO: Check that output dir exists

    linked_df = extract_from_table(config["table_names"]["input"]["merged_antibody_swab"])

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

    all_visits_output_df = map_output_values_and_column_names(linked_df, output_name_map, category_map)

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
            df = update_column_values_from_map(df, column_name_to_assign, map, complete_map)
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
