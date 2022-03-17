from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def sample_delta_ETL():
    extract_from_csv()
    validate_sample()

    df = edit_sample_file()
    calculate_design_weights()

    extract_existing_design_weights()

    load_updated_design_weights()
    return df


def extract_from_csv():
    pass


def validate_sample():
    pass


def edit_sample_file(df: DataFrame, sample_name: str, sample_direct: int) -> DataFrame:
    """
    Edit input sample delta to prepare for design weight calculation.

    Parameters
    ----------
    df
    sample_name
        identifier to apply to all records in (delta) sample
    sample_direct
        indicates whether sample was drawn from the address base
    """
    df = (
        df.withColumn("sample", F.lit(sample_name))
        .withColumn("sample_direct", F.lit(sample_direct))
        # Convert region codes to pseudocodes for Scotland and Wales
        .withColumn(
            "gor9d",
            F.when(F.col("custodian_region_code") == "S92000003", "S99999999")
            .when(F.col("custodian_region_code") == "W92000004", "W99999999")
            .otherwise(F.col("custodian_region_code")),
        )
        .drop("custodian_region_code")
        .withColumn(
            "country_sample",
            F.when(F.col("gor9d") == "W99999999", "Wales")
            .when(F.col("gor9d") == "S99999999", "Scotland")
            .when(F.col("gor9d") == "N99999999", "NI")
            .otherwise(F.lit("England")),
        )
        .withColumn("rgn", F.when(F.col("rgn").isNull(), F.col("gor9d")).otherwise(F.col("rgn")))
    )

    northwest_boost_list = [
        "E07000117",
        "E07000120",
        "E07000122",
        "E07000123",
        "E07000125",
        "E08000001",
        "E08000002",
        "E08000003",
        "E08000004",
        "E08000005",
        "E08000006",
        "E08000007",
        "E08000008",
        "E08000009",
        "E08000010",
    ]
    yorkshire_boost_list = ["E08000032", "E08000033", "E08000034"]
    df = df.withColumn(
        "gor9d_recoded",
        F.when((F.col("gor9d") == "E12000002") & (F.col("laua").isin(*northwest_boost_list)), "E12000002_boost")
        .when((F.col("gor9d") == "E12000002") & (~(F.col("laua").isin(*northwest_boost_list))), "E12000002_nonboost")
        .when((F.col("gor9d") == "E12000003") & (F.col("laua").isin(*yorkshire_boost_list)), "E12000003_boost")
        .when((F.col("gor9d") == "E12000003") & (~(F.col("laua").isin(*yorkshire_boost_list))), "E12000003_boost")
        .otherwise(F.col("gor9d")),
    )
    return df


def calculate_design_weights(sample_file: DataFrame, household_populations: DataFrame) -> DataFrame:
    """
    Calculate design weights, as the number of addresses within a CIS area (``interim_id``) over
    the number of households sampled within that area.

    Parameters
    ----------
    sample_file
        sample delta to calculate design weights per ``interim_id``
    household_populations
        number of addresses (``nb_addresses``) per CIS area (``interim_id``)
    """
    sample_file = sample_file.join(
        F.broadcast(household_populations.select("interim_id", "nb_addresses")), how="left", on="interim_id"
    )

    interim_id_window = Window.partitionBy("interim_id")
    sample_file = sample_file.withColumn("sample_count", F.count("interim_id").over(interim_id_window))

    sample_file = sample_file.withColumn("design_weight", F.col("nb_addresses") / F.col("sample_count"))
    return sample_file.drop("sample_count", "nb_addresses")


def extract_existing_design_weights():
    pass


def load_updated_design_weights():
    pass
