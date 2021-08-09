
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def edit_swab_results_single(df:DataFrame, gene_type:str, result_value:str) -> DataFrame:
    """
    The objective of this function is to correct the result_value from 1 to 0 (Positive to Negative) 
        in case gene_type is zero.

    Parameters
    ----------
    df : Pyspark Dataframe
    gene_type : String with categorical options "ctOR1ab","ctNgene","ctSgene".
    result_value : can only be the following discrete integer values 
        (0 - Negative, 1 - Positive, 7 - Rejected, 8 - Inconclusive, 9 - Void, 10 - Insuficient Sample)

    Returns
    -------
    df : Edited Pyspark Dataframe gene_type column with corrected values in case of wrong result_value
        with the following logic: (result_value == 1) & (gene_type == 0) & (result_mk == 1)
    """
    return df.withColumn(result_value, F.when(
                        # boolean logic:
                            (F.col(result_value) == 1) & (F.col(gene_type) <= 0) & (F.col('result_mk') == 1), 0
                        # if boolean condition not met, keep the same value.
                            ).otherwise(F.col(result_value)))

