from io import BytesIO
from typing import Dict

import pandas as pd
from pyspark.sql import DataFrame


def dfs_to_bytes_excel(sheet_df_map: Dict[str, DataFrame]):
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet, df in sheet_df_map.items():
            df.toPandas().to_excel(writer, sheet_name=sheet)
    return output
