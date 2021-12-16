import csv
from io import StringIO
from operator import add
from typing import List
from typing import Union

from pyspark import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.pyspark_utils import get_or_create_spark_session


spark_session = get_or_create_spark_session()

# fmt: off
resource_paths = {
    "old": {
        "path": r"C:\code\weights_inputs\old_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,tranche,sample,sample_direct,date _sample_created ,\
            batch_number,file_name,hh_dweight_swab,hh_dweight_atb,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "new": {
        "path": r"C:\code\weights_inputs\new_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,sample,sample_direct,date _sample_created ,\
            batch_number,file_name,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "nspl_lookup": {"path": r"C:\code\weights_inputs\lookup.csv", "header": "pcd,ctry,lsoa11"},
    "cis20cd_lookup": {
        "path": r"C:\code\weights_inputs\cis20lookup.csv",
        "header": "LSOA11CD,LSOA11NM,CIS20CD,RGN19CD",
    },
    "address_lookup": {
        "path": r"C:\code\weights_inputs\Address_lookup.csv",
        "header": "uprn,town_name,postcode,ctry18nm,la_code,ew,address_type,council_tax,udprn,address_base_postal",
    },
    "country_lookup": {
        "path": r"C:\code\weights_inputs\country_lookup.csv",
        "header": "LAD20CD,LAD20NM,CTRY20CD,CTRY20NM",
    },
    "tranche": {
        "path": r"C:\code\weights_inputs\tranche.csv",
        "header": "enrolement_date,UAC,lsoa_11,cis20cd,ctry12,ctry_name12,tranche",
    },
    "population_projection_current": {
        "path": r"C:\code\weights_inputs\population_projectionC.csv",
        "header": "laua,rgn,ctry,ctry_name,surge,m0,m25,f0,f25",
    },
    "population_projection_previous": {
        "path": r"C:\code\weights_inputs\population_projectionP.csv",
        "header": "laua,rgn,ctry,ctry_name,surge,m0,m25,f0,f25",
    },
    "aps_lookup": {
        "path": r"C:\code\weights_inputs\aps_lookup.csv",
        "header": "CASENO,COUNTRY,AGE,ETHGBEUL,ETH11NI,PWTA18"
    }
}

lookup_variable_name_maps = {
    "address_lookup": {
        "uprn": "unique_property_reference_code",
        "postcode": "postcode"},
    "nspl_lookup": {
        "pcd": "postcode",
        "lsoa11": "lower_super_output_area_code_11",
        "ctry": "country_code_12"},
    "cis20cd_lookup": {
        "LSOA11CD": "lower_super_output_area_code_11",
        "CIS20CD": "cis_area_code_20"},
    "country_lookup": {
        "CTRY20CD": "country_code_12",
        "CTRY20NM": "country_name_12"},
    "old_new": {
        "UAC": "ons_household_id",
        "lsoa_11": "lower_super_output_area_code_11",
        "cis20cd": "cis_area_code_20",
        "ctry12": "country_code_12",
        "ctry_name12": "country_name_12",
        "sample": "sample_source",
        "sample_direct": "sample_addressbase_indicator",
        "hh_dweight_swab": "household_level_designweight_swab",
        "hh_dweight_atb": "household_level_designweight_antibodies",
        "rgngor9d": "region_code",  # had to remove slashes to make df creation work should be rgn/gor9d
        "laua": "local_authority_unity_authority_code",
        "oa11oac11": "output_area_code_11/census_output_area_classification_11",  # had to remove slashes to make df creation work should be oa11/oac11 # noqa: E501
        "msoa11": "middle_super_output_area_code_11",
        "ru11ind": "rural_urban_classification_11",
        "imd": "index_multiple_deprivation",
    },
    "tranche": {
        "UAC": "ons_household_id",
        "lsoa_11": "lower_super_output_area_code_11",
        "cis20cd": "cis_area_code_20",
        "ctry12": "country_code_12",
        "ctry_name12": "country_name_12"
    },
    "population_projection_previous_population_projection_current": {
        "laua": "local_authority_unitary_authority_code",
        "rgn": "region_code",
        "ctry": "country_code_#",
        "ctry_name": "country_name_#",
    },
    "aps_lookup": {
        "caseno": "person_id_aps",
        "country": "country_name",
        "ethgbeul": "ethnicity_aps_engl_wales_scot",
        "eth11ni": "ethnicity_aps_northen_ireland",
        "pwta18": "person_level_weight_aps_18"
    }
}


aps_value_map = {
    "country_name": {
        1: "England",
        2: "Wales",
        3: "Scotland",
        4: "Scotland",
        5: "Northen Ireland",
    },
    "ethnicity_aps_engl_wales_scot": {
        1: "white british",
        2: "white irish",
        3: "other white",
        4: "mixed/multiple ethnic groups",
        5: "indian",
        6: "pakistani",
        7: "bangladeshi",
        8: "chinese",
        9: "any other asian  background",
        10: "black/ african /caribbean /black/ british",
        11: "other ethnic group",
    },
    "ethnicity_aps_northen_ireland": {
        1: "white",
        2: "irish traveller",
        3: "mixed/multiple ethnic groups",
        4: "asian/asian british",
        5: "black/ african/ caribbean /black british",
        6: "chinese",
        7: "arab",
        8: "other ethnic group",
    },
}
# fmt: on

# basic local csv reading----------------------


class InvalidFileError(Exception):
    pass


def validate_csv_fields(text_file: RDD, delimiter: str = ","):
    """
    Function to validate the number of fields within records of a csv file.
    Parameters
    ----------
    text_file
        A text file (csv) that has been ready by spark context
    delimiter
        Delimiter used in csv file, default as ','
    """

    def count_fields_in_row(delimiter, row):
        f = StringIO(row)
        reader = csv.reader(f, delimiter=delimiter)
        n_fields = len(next(reader))
        return n_fields

    header = text_file.first()
    number_of_columns = count_fields_in_row(delimiter, header)
    error_count = text_file.map(lambda row: count_fields_in_row(delimiter, row) != number_of_columns).reduce(add)
    return True if error_count == 0 else False


def validate_csv_header(text_file: RDD, expected_header: str):
    """
    Function to validate header in csv file matches expected header.

    Parameters
    ----------
    text_file
        A text file (csv) that has been ready by spark context
    expected_header
        Exact header expected in csv file
    """
    header = text_file.first()
    return expected_header == header


def read_csv_to_pyspark_df(
    spark_session: SparkSession,
    csv_file_path: Union[str, list],
    expected_raw_header_row: str,
    schema: StructType,
    sep: str = ",",
    **kwargs,
) -> DataFrame:
    """
    Validate and read a csv file into a PySpark DataFrame.

    Parameters
    ----------
    csv_file_path
        file to read to dataframe
    expected_raw_header_row
        expected first line of file
    schema
        schema to use for returned dataframe, including desired column names

    Takes keyword arguments from ``pyspark.sql.DataFrameReader.csv``,
    for example ``timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'"``.
    """
    spark_session = get_or_create_spark_session()
    if not isinstance(csv_file_path, list):
        csv_file_path = [csv_file_path]

    for csv_file in csv_file_path:
        text_file = spark_session.sparkContext.textFile(csv_file)
        csv_header = validate_csv_header(text_file, expected_raw_header_row)
        csv_fields = validate_csv_fields(text_file, delimiter=sep)

        if not csv_header:
            raise InvalidFileError(
                f"Header of {csv_file} ({text_file.first()}) "
                f"does not match expected header: {expected_raw_header_row}"
            )

        if not csv_fields:
            raise InvalidFileError(
                f"Number of fields in {csv_file} does not match expected number of columns from header"
            )

    return spark_session.read.csv(
        csv_file_path,
        header=True,
        schema=schema,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=sep,
        **kwargs,
    )


# --------------------------------------------


def load_auxillary_data(specify: List = []):
    """
    create dictionary of renamed dataframes after extracting from csv file
    """
    auxillary_dfs = {}
    for name, resource_path in resource_paths.items():
        if specify == [] or name in specify:
            auxillary_dfs[name] = read_csv_to_pyspark_df(
                spark_session, resource_path["path"], resource_path["header"], None
            )
    return auxillary_dfs


def prepare_auxillary_data(auxillary_dfs: dict):
    auxillary_dfs = rename_columns(auxillary_dfs)
    if "aps_lookup" in auxillary_dfs:
        auxillary_dfs["aps_lookup"] = recode_column_values(auxillary_dfs["aps_lookup"], aps_value_map)
    return auxillary_dfs


def rename_columns(auxillary_dfs: dict):
    """
    iterate over keys in name map dictionary and use name map if name of df is in key.
    break out of name checking loop once a compatible name map has been found.
    """
    for name in auxillary_dfs.keys():
        for name_list_str in lookup_variable_name_maps.keys():
            if name in name_list_str:
                for old_name, new_name in lookup_variable_name_maps[name_list_str].items():
                    auxillary_dfs[name] = auxillary_dfs[name].withColumnRenamed(old_name, new_name)
                break
    return auxillary_dfs


def recode_column_values(df: DataFrame, lookup: dict):
    for column_name, map in lookup.items():
        df = update_column_values_from_map(df, column_name, map)
    return df
