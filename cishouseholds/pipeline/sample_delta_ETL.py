from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()


def sample_delta_ETL():
    extract_from_csv()
    validate_sample()

    edit_sample_file()
    calculate_design_weights()

    extract_existing_design_weights()

    load_updated_design_weights()


def extract_from_csv():
    """
    reads in the households, lookup, previous sample and new sample direct files
    from csv files stored in the working tree

    outputs:
    dictionary of named dataframed obejcts with values corresponding to each .csv file
    """
    input_schema = """uac string, la_code string, bloods integer, oa11 string, laua	string, ctry string,\
        custodian_region_code string, lsoa11 string, msoa11 string,	ru11ind	string, oac11 string,\
        rgn	string, imd	integer, interim_id integer"""
    # file_path = "raw_households\ons_gl_report\design_weights_input\sample_direct_eng_wc280621.csv"
    raw_header = (
        "uac,la_code,bloods,oa11,laua,ctry,custodian_region_code,lsoa11,msoa11,ru11ind,oac11,rgn,imd,interim_id"
    )

    household_schema = """interim_id integer, nb_addresses integer,	CIS20CD string"""
    # household_file_path = "raw_households\ons_gl_report\design_weights_input\household_nb_by_cis_id_Jun21.csv"
    household_header = "interim_id,nb_addresses,CIS20CD"

    previous_schema = """sample	string, uac string,	region string, dvhsize integer, \
        country_sample string, laua	string, rgn	string, lsoa11 string, msoa11 string, imd integer,\
        interim_id integer,	dweight_hh double, sample_direct integer, tranche string, dweight_hh_atb double"""
    # previous_file_path = "raw_households\ons_gl_report\design_weights_input\Previous samples and weights.csv"
    previous_header = "sample,uac,region,dvhsize,country_sample,laua,rgn,lsoa11,msoa11,imd,interim_id,dweight_hh,\
        sample_direct,tranche,dweight_hh_atb"

    lookup_schema = (
        """LSOA11CD string,	LSOA11NM string, CIS20CD string, RGN19CD string, imd string, interim_id string"""
    )
    # lookup_file_path = "raw_households\ons_gl_report\design_weights_input\lsoa_cis_imd_lookup_14Jun21.csv"
    lookup_header = "LSOA11CD,LSOA11NM,CIS20CD,RGN19CD,imd,interim_id"

    dataframes = {}

    # setup empty vars because flake 8 is bad
    previous_file_path = ""
    lookup_file_path = ""
    household_file_path = ""
    file_path = ""

    dataframes["previous"] = read_csv_to_pyspark_df(
        spark_session,
        previous_file_path,
        previous_header,
        previous_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )
    dataframes["lookup"] = read_csv_to_pyspark_df(
        spark_session,
        lookup_file_path,
        lookup_header,
        lookup_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )
    dataframes["household"] = read_csv_to_pyspark_df(
        spark_session,
        household_file_path,
        household_header,
        household_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )

    dataframes["sample_direct"] = read_csv_to_pyspark_df(
        spark_session,
        file_path,
        raw_header,
        input_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )
    return dataframes


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
        household_populations.select("interim_id", "CIS20CD", "nb_addresses"), how="left", on="interim_id"
    )

    interim_id_window = Window.partitionBy("interim_id")
    sample_file = sample_file.withColumn("sample_size", F.count("interim_id").over(interim_id_window))

    sample_file = sample_file.withColumn("dweight_hh", F.col("nb_addresses") / F.col("sample_size"))
    return sample_file.drop("nb_addresses")


def extract_existing_design_weights():
    pass


def load_updated_design_weights():
    pass
