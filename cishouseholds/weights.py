from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def household_design_weights(df_address_base: DataFrame, df_nspl: DataFrame, df_lsoa: DataFrame) -> DataFrame:
    """
    Join address base and nspl (National Statistics Postcode Lookup) to then join
    lsoa (Lower Level Output Area) to get household count.
    df_address_base
        Dataframe with
    df_nspl
        Dataframe linking postcodes
    df_lsoa
        Dataframe with
    """
    df = (
        df_address_base.join(df_nspl, df_address_base.postcode == df_nspl.pcd, "inner")
        .withColumn("postcode", F.regexp_replace(F.col("postcode"), " ", ""))
        .drop("pcd")
    )

    df = df.join(df_lsoa, df.lsoa11 == df_lsoa.lsoa11cd, "inner").drop("lsoa11cd")

    return (
        df.groupBy("interim_id", "cis20cd", "uprn")
        .count()
        .drop("uprn")
        .withColumnRenamed("count", "nb_address")
        .withColumn(
            "nb_address", F.when(F.col("nb_address").isNotNull(), F.col("nb_address").cast("int")).otherwise(None)
        )
    )
