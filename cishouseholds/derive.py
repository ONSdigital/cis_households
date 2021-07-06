from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_column_from_coalesce(df, new_column_name, *args):
    """
    Create new column with values from coalesced columns.
    From households_aggregate_processes.xlsx, derivation number 6.
    D6: V1, or V2 if V1 is missing

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    new_column_name: string
    *args: string
        name of columns to coalesce

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    return df.withColumn(colName=new_column_name, col=F.coalesce(*args))


def substring_column(df: DataFrame, new_column_name, column_to_substr, start_position, len_of_substr):
    """
    Criteria - returns data with new column which is a substring
    of an existing variable
    Parameters
    ----------
    df: pyspark.sql.DataFrame
    new_column_name: string
    column_to_substr: string
    start_position: integer
    len_of_substr: integer

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    df = df.withColumn(new_column_name, F.substring(column_to_substr, start_position, len_of_substr))

    return df


def derive_ctpattern(df: DataFrame, spark_session):
    """
    Derive a new column containing string of pattern in
    ["N only", "OR only", "S only", "OR+N", "OR+S", "N+S", "OR+N+S", NULL]
    indicating which ct_* columns indicate a positive result.
    From households_aggregate_processes.xlsx, derivation number 7.

    Parameters
    ----------
    spark_session: pyspark.sql.SparkSession
    df: pyspark.sql.DataFrame

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    lookup_df = spark_session.createDataFrame(
        data=[
            ("0", "0", "0", "NULL"),
            ("1", "0", "0", "OR only"),
            ("0", "1", "0", "N only"),
            ("0", "0", "1", "S only"),
            ("1", "1", "0", "OR+N"),
            ("1", "0", "1", "OR+S"),
            ("0", "1", "1", "N+S"),
            ("1", "1", "1", "OR+N+S"),
        ],
        schema=["indicator_ct_or", "indicator_ct_n", "indicator_ct_s", "ctpattern"],
    )

    on_condition = []
    for ct_gene in ["or", "n", "s"]:
        df = df.withColumn(f"indicator_ct_{ct_gene}", F.when(F.col(f"cf_{ct_gene}") > 0, "1").otherwise("0"))

        on_condition.append(f"indicator_ct_{ct_gene}")

    df = df.join(F.broadcast(lookup_df), on=on_condition, how="left").drop(
        "indicator_ct_or", "indicator_ct_n", "indicator_ct_s"
    )

    return df


def derive_ctpattern2(df: DataFrame):
    df = df.withColumn(
        "ct_pattern",
        F.when((F.col("ct_or") > 0) & (F.col("ct_n") == 0) & (F.col("ct_s") == 0), "OR only")
        .when((F.col("ct_or") == 0) & (F.col("ct_n") > 0) & (F.col("ct_s") == 0), "N only")
        .when((F.col("ct_or") == 0) & (F.col("ct_n") == 0) & (F.col("ct_s") > 0), "S only")
        .when((F.col("ct_or") > 0) & (F.col("ct_n") > 0) & (F.col("ct_s") == 0), "OR+N")
        .when((F.col("ct_or") > 0) & (F.col("ct_n") == 0) & (F.col("ct_s") > 0), "OR+S")
        .when((F.col("ct_or") == 0) & (F.col("ct_n") > 0) & (F.col("ct_s") > 0), "N+S")
        .when((F.col("ct_or") > 0) & (F.col("ct_n") > 0) & (F.col("ct_s") > 0), "N+S")
        .otherwise("NULL"),
    )
