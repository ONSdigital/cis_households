from io import BytesIO
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def dfs_to_bytes_excel(sheet_df_map: Dict[str, DataFrame]):
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet, df in sheet_df_map.items():
            df.toPandas().to_excel(writer, sheet_name=sheet)
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
