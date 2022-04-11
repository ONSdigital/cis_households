import re

from pyspark.sql import DataFrame

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()


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
    "ethnicity_aps_england_wales_scotland": {
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


def recode_column_values(df: DataFrame, lookup: dict):
    for column_name, map in lookup.items():
        df = update_column_values_from_map(df, column_name, map)
    return df
