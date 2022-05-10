from io import BytesIO
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.merge import union_multiple_tables


def dfs_to_bytes_excel(sheet_df_map: Dict[str, DataFrame]):
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet, df in sheet_df_map.items():
            df.toPandas().to_excel(writer, sheet_name=sheet, index=False)
    return output


def multiple_visit_1_day(df, participant_id, visit_id, date_column, datetime_column):
    """
    Parameters
    ----------
    df
    participant_id
    visit_id
    date_column
    datetime_column
    """
    window = Window.partitionBy(participant_id, date_column)  # .orderBy(date_column, datetime_column)

    df = df.withColumn("FLAG", F.count(visit_id).over(window))
    df_multiple_visit = df.filter(F.col("FLAG") > 1)  # get only multiple visit
    df_multiple_visit = df_multiple_visit.withColumn(
        "FLAG", F.rank().over(window.orderBy(date_column, F.desc(datetime_column)))
    )
    df_multiple_visit = df_multiple_visit.filter(F.col("FLAG") == 1)
    return df_multiple_visit.drop("FLAG")


def unmatching_antibody_to_swab_viceversa(swab_df, antibody_df, column_list):
    """
    Parameters
    ----------
    swab_df
    antibody_df
    column_list
    """
    unmatched_swab_df = swab_df.join(antibody_df, on="barcode", how="left_anti").select(*column_list)
    unmatched_antibody_df = antibody_df.join(swab_df, on="barcode", how="left_anti").select(*column_list)

    df = union_multiple_tables(tables=[unmatched_swab_df, unmatched_antibody_df])
    return df
