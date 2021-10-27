from typing import Any
from typing import List
from typing import Optional
from typing import Union

from pyspark.sql import DataFrame

from cishouseholds.edit import update_column_values_from_map


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
