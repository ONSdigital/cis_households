from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import household_population_variable_name_map


def household_population_total(
    df_address_base: DataFrame, df_nspl: DataFrame, df_lsoa: DataFrame, df_country_code: DataFrame
):
    # Join all 4 datasets
    df = df_address_base.join(df_nspl, df_address_base.postcode == df_nspl.pcd, "left")
    df = df.join(df_lsoa, df.lsoa11cd == df_lsoa.lsoa11cd, "left")
    df = df.join(df_country_code, df.ctry12cd == df_country_code.ctry20cd, "left").withColumn(
        "postcode", F.regexp_replace(F.col("postcode"), " ", "")
    )

    # select only columns needed for aggregation and output dataframe
    df = df.select("uprn", "postcode", "lsoa11cd", "cis20cd", "ctry12cd", "ctry20nm").distinct

    # count column for household population by cis and country code 12
    w1 = Window.partitionBy("cis20cd")
    w2 = Window.partitionBy("ctry20cd")

    df = df.withColumn("nhp_cis", F.count("uprn").over(w1)).withColumn("nhp_cc12", F.count("uprn").over(w2))

    # Rename columns based on map

    df = rename_column_names(df, household_population_variable_name_map)

    return df.drop("uprn")
