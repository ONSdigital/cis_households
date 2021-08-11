
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def edit_swab_results_single(
                                df:DataFrame, 
                                gene_result_classification:str, 
                                gene_result_value:str, 
                                overall_result_classification:str) -> DataFrame:
    """
    The objective of this function is to edit/correct the gene_result_classification from Positive to Negative or 1 to 0
        in case gene_result_value is 0.0 or lower and overall_result_classification is Positive or 1.

    Parameters
    ----------
    df 
    gene_result_classification
    gene_result_value
        column name that consists of float values
    overall_result_classification
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

