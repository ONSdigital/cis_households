
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def edit_swab_results_single(
                                df:DataFrame, 
                                gene_result_classification:str, 
                                gene_result_value:str, 
                                overall_result_classification:str='result_mk') -> DataFrame:
    """
    The objective of this function is to edit/correct the gene_result_classification from Positive to Negative or 1 to 0
        in case gene_result_value is 0.0 or lower and overall_result_classification is Positive or 1.

    Parameters
    ----------
    df : Pyspark Dataframe
    gene_result_classification : String with categorical options "ctOR1ab","ctNgene","ctSgene".
        can only be the following discrete integer values 
        (0 - Negative, 1 - Positive, 7 - Rejected, 8 - Inconclusive, 9 - Void, 10 - Insuficient Sample)
    gene_result_value : a float value that can go from 0.0 to 40.00 (aprox.)
    overall_result_classification : by default set to result_mk

    Returns
    -------
    df : Edited Pyspark Dataframe gene_result_classification column with corrected values in case of wrong gene_result_value
        with the following logic: (gene_result_value == 1) & (gene_result_classification == 0) & (result_mk == 1)
    """
    return df.withColumn(
                        gene_result_classification, 
                        F.when( 
                        # boolean logic:
                            (F.col(gene_result_classification) == 'Positive') & 
                            (F.col(gene_result_value) <= 0.0) & 
                            (F.col(overall_result_classification) == 'Positive'), 'Negative'
                        # if boolean condition not met, keep the same value.
                            ).otherwise(F.col(gene_result_classification))
                        )

