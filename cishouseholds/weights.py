from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def household_design_weights(df_address_base: DataFrame, df_nspl: DataFrame, df_lsoa: DataFrame) -> DataFrame:
    # step 2
    # add LSOA11 to address base file using postcode(address_base file)/pcd (NSPL file)
    # when matching delete the spaces from postcodes
    df = (
        df_address_base.join(df_nspl, df_address_base.postcode == df_nspl.pcd, "inner")
        .withColumn("postcode", F.regexp_replace(F.col("postcode"), " ", ""))
        .drop("pcd")
    )

    # step 3
    # add CIS20CD to address_base_file using lsoa11 ( address_base_file/LSOA11CD (LSOA to CIS area lookup)
    df = df.join(df_lsoa, df.lsoa11 == df_lsoa.lsoa11cd, "inner").drop("lsoa11cd")

    # step 4
    # use the address_base file _step3 AND produce a ne file called household populations(csv)
    # group by CIS20CD and count uprn unique values
    return (
        df.groupBy("cis20cd", "uprn")
        .count()
        .drop("uprn")
        .withColumnRenamed("count", "nb_address")
        .select("nb_address", "cis20cd")
        .withColumn(
            "nb_address", F.when(F.col("nb_address").isNotNull(), F.col("nb_address").cast("int")).otherwise(None)
        )
    )
