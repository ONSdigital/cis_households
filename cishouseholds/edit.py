
from pyspark.sql.functions import when, col
from pyspark.sql import DataFrame

def edit_swab_results_single(df:DataFrame, V2:str) -> DataFrame:
    """
    Parameters
    ----------
    df : Pyspark Dataframe
    V2 : String with categorical options for "ctOR1ab","ctNgene","ctSgene".

    Returns
    -------
    df : Edited Pyspark Dataframe with corrected values in case of wrong result
        with the following logic: 
    """
    return df.withColumn(V2 + '_result', when((col(V2 + '_result') == 1) & (col(V2) <= 0) & (col('result_mk') == 1), 0)\
                                        .otherwise(col(V2 + '_result')))

