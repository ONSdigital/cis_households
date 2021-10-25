from typing import Any
from typing import List
from typing import Optional
from typing import Union

from pyspark.sql import DataFrame

from cishouseholds.edit import update_column_values_from_map


def configure_outputs(
    df: DataFrame,
    selection_columns: Optional[Union[List[str], str]] = None,
    group_by_columns: Optional[Union[List[str], str]] = None,
    aggregate_function: Optional[Any] = None,
    aggregate_column_name: Optional[str] = None,
    name_map: Optional[dict] = None,
    value_map: Optional[dict] = None,
):
    """
    Customise the output of the pipeline using user inputs
    Parameters
    ----------
    df
    selection_columns
    group_by_columns
    name_map
    """
    try:
        for check in group_by_columns, name_map.keys(), value_map.keys():  # type: ignore
            try:
                for column in check:  # type: ignore
                    if column not in selection_columns:  # type: ignore
                        selection_columns.append(column)  # type: ignore
            except TypeError:
                pass
    except Exception:
        pass
    if type(group_by_columns) != list and group_by_columns is not None:
        group_by_columns = [str(group_by_columns)]
    if type(selection_columns) != list and selection_columns is not None:
        selection_columns = [str(selection_columns)]
    if selection_columns:
        df = df.select(*selection_columns)
    if group_by_columns:
        if aggregate_column_name:
            prev_cols = set(df.columns)
            df = df.groupBy(*group_by_columns).agg({"*": aggregate_function})
            new_col = list(set(df.columns) - prev_cols)[0]
            df = df.withColumnRenamed(new_col, aggregate_column_name)
        else:
            df = df.groupBy(*group_by_columns).agg({"*": aggregate_function})
    if name_map:
        for current_name, to_be_name in name_map.items():
            df = df.withColumnRenamed(current_name, to_be_name)
    if value_map:
        for column_name_to_assign, map in value_map.items():
            df = update_column_values_from_map(df, column_name_to_assign, map)
    return df
