from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calc_raw_design_weight_ab(df: DataFrame):
    """
    Calculate raw design weight variable for scenario a/b
    Parameters
    ----------
    dataframe
    """
    return df.withColumn(
        "raw_design_weights_antibodies_ab",
        F.when(
            (F.col("sample_new_previous") == "previous") & (F.col("household_designweigh_antibodies").isNotNull()),
            df["household_designweigh_antibodies"],
        )
        .when(
            (F.col("sample_new_previous") == "new") & (F.col("household_designweigh_antibodies").isNull()),
            df["scaled_design_weight_swab_nonadjusted"],
        )
        .otherwise(None),
    )
