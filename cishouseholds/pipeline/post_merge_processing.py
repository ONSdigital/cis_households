from itertools import chain
from typing import Any
from typing import List
from typing import Optional
from typing import Union

import pyspark.sql.functions as F
from _typeshed import NoneType
from pyspark.sql import DataFrame

from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


@register_pipeline_stage("process_post_merge")
def process_post_merge():
    pass


def configure_outputs(
    df: DataFrame,
    selection_columns: Optional[Union[List[str], str, NoneType]] = None,
    group_by_columns: Optional[Union[List[str], str, NoneType]] = None,
    aggregate_function: Optional[Union[Any, NoneType]] = None,
    name_map: Optional[dict] = {},
    value_map: Optional[dict] = {},
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
    except TypeError:
        pass
    if selection_columns:
        df = df.select(*selection_columns)
    if group_by_columns:
        df = df.groupBy(*group_by_columns).agg(aggregate_function)
    if name_map:
        for current_name, to_be_name in name_map.items():
            df = df.withColumnRenamed(current_name, to_be_name)
    if value_map:
        for column_name_to_assign, map in value_map.items():
            mapping_expr = F.create_map([F.lit(x) for x in chain(*map.items())])  # type: ignore
            df = df.withColumn(column_name_to_assign, mapping_expr[df[column_name_to_assign]])
    return df
