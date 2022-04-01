import re

from pyspark.sql import DataFrame

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()

sample_file_column_map = {
    "UAC": "ons_household_id",
    "uac": "ons_household_id",
    "lsoa_11": "lower_super_output_area_code_11",
    "lsoa11": "lower_super_output_area_code_11",
    "cis20cd": "cis_area_code_20",
    "CIS20CD": "cis_area_code_20",
    "ctry12": "country_code_12",
    "ctry": "country_code_12",
    "ctry_name12": "country_name_12",
    "sample": "sample_source",
    "sample_direct": "sample_addressbase_indicator",
    "rgn/gor9d": "region_code",
    "rgn": "region_code",
    "gor9d": "region_code",
    "laua": "local_authority_unity_authority_code",
    "oa11oac11": "output_area_code_11_census_output_area_classification_11",
    "oa11": "output_area_code_11_census_output_area_classification_11",
    "msoa11": "middle_super_output_area_code_11",
    "ru11ind": "rural_urban_classification_11",
    "imd": "index_multiple_deprivation",
    "date_sample_created": "date_sample_created",
    "batch_number": "batch_number",
}

projections_column_map = {
    "laua": "local_authority_unitary_authority_code",
    "rgn": "region_code",
    "ctry": "country_code_12",
    "ctry_name": "country_name_12",
    # TODO: check these names are correctly mapped
    "laname_21": "local_authority_unitary_authority_name",
    "ladcode_21": "local_authority_unitary_authority_code",
    "region9charcode": "region_code",
    "regionname": "region_name",
    "country9charcode_09": "country_code_12",
    "countryname_09": "country_name_12",
}

lookup_variable_name_maps = {
    "address_lookup": {
        "uprn": "unique_property_reference_code",
        "postcode": "postcode",
        "town_name": "town_name",
        "ctry18nm": "crtry18nm",
        "la_code": "la_code",
        "ew": "ew",
        "address_type": "address_type",
        "council_tax": "council_tax",
        "address_base_postal": "address_base_postal",
        "UPRN": "unique_property_reference_code",
        "POSTCODE": "postcode",
        "TOWN_NAME": "town_name",
        "CTRY18NM": "crtry18nm",
        "LA_CODE": "la_code",
        "EW": "ew",
        "ADDRESS_TYPE": "address_type",
        "COUNCIL_TAX": "council_tax",
        "ADDRESSBASE_POSTAL": "address_base_postal",
    },
    "postcode_lookup": {"pcd": "postcode", "lsoa11": "lower_super_output_area_code_11", "ctry": "country_code_12"},
    "cis_phase_lookup": {
        "LSOA11CD": "lower_super_output_area_code_11",
        "CIS20CD": "cis_area_code_20",
        "LSOA11NM": "LSOA11NM",
        "RGN19CD": "RGN19CD",
    },
    "country_lookup": {
        "CTRY20CD": "country_code_12",
        "CTRY20NM": "country_name_12",
        "LAD20CD": "LAD20CD",
        "LAD20NM": "LAD20NM",
    },
    "old_sample_file": {
        **sample_file_column_map,
        **{
            "hh_dweight_swab": "household_level_designweight_swab",
            "dweight_hh": "household_level_designweight_swab",
            "dweight_hh_atb": "household_level_designweight_antibodies",
            "hh_dweight_atb": "household_level_designweight_antibodies",
            "tranche": "tranche",
        },
    },
    "new_sample_file": sample_file_column_map,
    "tranche": {
        "UAC": "ons_household_id",
        "lsoa_11": "lower_super_output_area_code_11",
        "cis20cd": "cis_area_code_20",
        "ctry12": "country_code_12",
        "ctry_name12": "country_name_12",
        "enrolment_date": "enrolment_date",
        "tranche": "tranche",
    },
    "population_projection_previous": projections_column_map,
    "population_projection_current": projections_column_map,
    "aps_lookup": {
        "caseno": "person_id_aps",
        "country": "country_name",
        "ethgbeul": "ethnicity_aps_engl_wales_scot",
        "eth11ni": "ethnicity_aps_northen_ireland",
        "pwta18": "person_level_weight_aps_18",
        "age": "age",
    },
}

numeric_column_pattern_map = {
    "^losa_\d{1,}": "lower_super_output_area_code_{}",  # noqa:W605
    "^lsoa\d{1,}": "lower_super_output_area_code_{}",  # noqa:W605
    "^CIS\d{1,}CD": "cis_area_code_{}",  # noqa:W605
    "^cis\d{1,}cd": "cis_area_code_{}",  # noqa:W605
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


def prepare_auxillary_data(auxillary_dfs: dict):
    auxillary_dfs = rename_and_remove_columns(auxillary_dfs)
    if "aps_lookup" in auxillary_dfs:
        auxillary_dfs["aps_lookup"] = recode_column_values(auxillary_dfs["aps_lookup"], aps_value_map)
    return auxillary_dfs


def recode_column_patterns(df: DataFrame):
    for curent_pattern, new_prefix in numeric_column_pattern_map.items():
        col = list(filter(re.compile(curent_pattern).match, df.columns))
        if len(col) > 0:
            col = col[0]
            new_col = new_prefix.format(re.findall(r"\d{1,}", col)[0])  # type: ignore
            df = df.withColumnRenamed(col, new_col)
    return df


def rename_and_remove_columns(auxillary_dfs: dict):
    """
    iterate over keys in name map dictionary and use name map if name of df is in key.
    break out of name checking loop once a compatible name map has been found.
    """
    for name in auxillary_dfs.keys():
        if auxillary_dfs[name] is not None:
            if name not in ["population_projection_current", "population_projection_previous"]:
                auxillary_dfs[name] = auxillary_dfs[name].drop(
                    *[col for col in auxillary_dfs[name].columns if col not in lookup_variable_name_maps[name].keys()]
                )
            for old_name, new_name in lookup_variable_name_maps[name].items():
                auxillary_dfs[name] = auxillary_dfs[name].withColumnRenamed(old_name, new_name)
    validate_columns(auxillary_dfs)
    return auxillary_dfs


def validate_columns(dfs: DataFrame):
    for name, df in dfs.items():
        cols = list(dict.fromkeys(lookup_variable_name_maps[name].values()))
        if df is not None and not all(item in df.columns for item in cols):
            non_exist = [col for col in cols if col not in df.columns]
            raise ImportError(f"input dataframe {name} is missing columns {non_exist}")


def recode_column_values(df: DataFrame, lookup: dict):
    for column_name, map in lookup.items():
        df = update_column_values_from_map(df, column_name, map)
    return df
