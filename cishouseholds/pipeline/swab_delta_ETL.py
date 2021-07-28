from pyspark.sql import functions as F



from cis_households.tests.derive.test_assign_from_lookup import lookup_df
from ..derive import assign_column_convert_to_date, assign_from_lookup, mean_across_columns
from ..derive import derive_ctpattern

# from ..derive import assign_from_lookup
# from ..derive import mean_across_columns


def swabs_delta_ETL():
    extract_swabs_delta()
    transform_swabs_delta()
    load_swabs_delta()


def extract_swabs_delta():
    pass


def transform_swabs_delta(df, spark_session):
    """
    Call functions to process input for swabs deltas.

    D13: assign_column_convert_to_date
        derived variable name: result_mk_date_time
        V1-Vn: result_mk_date_time
    D7: derive_ctpattern
        derived variable name: ctpattern
        V1-Vn: ctORF1ab_result,ctNgene_result,ctSgene_result
    D9: mean_across_columns
        derived variable name: ct_mean
        V1-Vn: ctpattern,ctORF1ab,ctNgene,ctSgene

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    spark_session: pyspark.sql.SparkSession

    Return
    ------
    df: pyspark.sql.DataFrame
    """

    df = assign_column_convert_to_date(df, "result_mk_date", "result_mk_date_time")
    df = derive_ctpattern(df, ["ctORF1ab", "ctNgene", "ctSgene"], spark_session)
    df = mean_across_columns(df, "ct_mean", ["ctpattern", "ctORF1ab", "ctNgene", "ctSgene"])
    df = df.withColumn("ctonetarget", F.when(F.col("ctpattern").isin(["N only", "OR only", "S only"]),1)
                                    .when(F.col("ctpattern").isin(["OR+N", "OR+S", "N+S", "OR+N+S"]),0)
                                    .otherwise(None))
    return df

def load_swabs_delta():
    pass
