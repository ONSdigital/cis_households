from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from ..derive import assign_column_convert_to_date
from ..derive import assign_isin_list
from ..derive import derive_ctpattern
from ..derive import mean_across_columns


def swabs_delta_ETL():
    extract_swabs_delta()
    transform_swabs_delta()
    load_swabs_delta()


def extract_swabs_delta():
    pass


def transform_swabs_delta(df: DataFrame, spark_session: SparkSession) -> DataFrame:
    """
    Call functions to process input for swabs deltas.

    Parameters
    ----------
    df
        swabs deltas
    spark_session

    Return
    ------
    pyspark.sql.DataFrame

    Notes
    -----
    Functions implemented:
        D13: assign_column_convert_to_date
            derived variable name: result_mk_date_time
            V1-Vn: result_mk_date_time
        D7: derive_ctpattern
            derived variable name: ctpattern
            V1-Vn: ctORF1ab_result,ctNgene_result,ctSgene_result
        D9: mean_across_columns
            derived variable name: ct_mean
            V1-Vn: ctpattern,ctORF1ab,ctNgene,ctSgene
        D10: assign_isin_list
            derived variable name: ctonetarget
            V1-Vn: ctpattern
    """

    df = assign_column_convert_to_date(df, "result_mk_date", "result_mk_date_time")
    df = derive_ctpattern(df, ["ctORF1ab", "ctNgene", "ctSgene"], spark_session)
    df = mean_across_columns(df, "ct_mean", ["ctORF1ab", "ctNgene", "ctSgene"])
    df = assign_isin_list(df, "ctonetarget", "ctpattern", ["N only", "OR only", "S only"])

    return df


def load_swabs_delta():
    pass
