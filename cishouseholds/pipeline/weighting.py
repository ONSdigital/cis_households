from typing import Any
from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()

variable_name_map = {
    "UAC": "ons_household_id",
    "lsoa_11": "lower_super_output_area_code_11",
    "cis20cd": "cis_area_code_20",
    "ctry12": "country_code_12",
    "ctry_name12": "country_name_12",
    "tranche": "tranche_number_indicator",
    "sample": "sample_source",
    "sample_direct": "sample_addressbase_indicator",
    "hh_dweight_swab": "household_level_designweight_swab",
    "hh_dweight_atb": "household_level_designweight_antibodies",
    "rgn/gor9d": "region_code",
    "laua": "local_authority_unity_authority_code",
    "oa11/oac11": "output_area_code_11/census_output_area_classification_11",
    "msoa11": "middle_super_output_area_code_11",
    "ru11ind": "rural_urban_classification_11",
    "imd": "index_multiple_deprivation",
}


def load_auxillary_data(resource_paths):
    auxillary_dfs = {}
    for name, resource_path in resource_paths.items():
        auxillary_dfs[name] = read_csv_to_pyspark_df(
            spark_session, resource_path["path"], resource_path["header"], None
        )
    df = get_if_MATCHED(
        old_sample_df=auxillary_dfs["old"], new_sample_df=auxillary_dfs["new"], selection_columns=["lsoa_11", "cis20cd"]
    )
    df = update_data(df, auxillary_dfs)
    df = clean_and_rename(df, variable_name_map)
    old_df = clean_and_rename(auxillary_dfs["old"], variable_name_map)
    df = union_multiple_tables(tables=[df, old_df])
    df.toPandas().to_csv("test_output3.csv", index=False)
    df = assign_sample_new_previous(df, "sample_new_previous", "date _sample_created", "batch_number")
    df = df.join(auxillary_dfs["tranche"], auxillary_dfs["tranche"].UAC == df.ons_household_id, how="outer").drop("UAC")
    df = assign_tranche_factor(df, "tranche_factor", "ons_household_id", ["cis_area_code_20", "enrolement_date"])


def get_if_MATCHED(old_sample_df: DataFrame, new_sample_df: DataFrame, selection_columns: List[str]):
    join_on_column = "uac"
    select_df = old_sample_df.select(join_on_column, *selection_columns)
    for col in select_df.columns:
        if col != join_on_column:
            select_df = select_df.withColumnRenamed(col, col + "_OLD")
    joined_df = new_sample_df.join(select_df, on=join_on_column, how="left")
    for col in selection_columns:
        joined_df = joined_df.withColumn(f"MATCHED_{col}", F.when(F.col(col) == F.col(f"{col}_OLD"), 1).otherwise(None))
    joined_df.show()
    return joined_df


def update_data(df: DataFrame, auxillary_dfs: dict):
    lsoa_lookup_df = auxillary_dfs["lsoa_lookup"]
    cis20_lookup_df = auxillary_dfs["cis20_lookup"]
    df = update_lsoa(df, lsoa_lookup_df, "lsoa_11", "ctry12", "postcode")
    df = update_cis20cd(df, cis20_lookup_df, "cis20cd", "lsoa_11")
    drop_columns = [col for col in df.columns if "MATCHED" in col]
    return df.drop(*drop_columns)


def update_lsoa(
    df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, country_column: str, postcode_column: str
):
    lookup_df = (
        lookup_df.withColumnRenamed("ctry", country_column)
        .withColumnRenamed("pcd", postcode_column)
        .withColumnRenamed("lsoa11", "lsoa_11_from_lookup")
    )
    df = df.join(lookup_df, on=[country_column, postcode_column], how="left")
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(f"MATCHED_{column_name_to_update}").isNull(),
            F.when(F.col("lsoa_11_from_lookup").isNotNull(), F.col("lsoa_11_from_lookup")).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop("lsoa_11_from_lookup")


def update_cis20cd(df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, lsoa_column: str):
    lookup_df = lookup_df.withColumnRenamed("LSOA11CD", lsoa_column).withColumnRenamed("CIS20CD", "cis20cd_from_lookup")
    df = df.join(lookup_df.select(lsoa_column, "cis20cd_from_lookup"), on=lsoa_column, how="left")
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(f"MATCHED_{column_name_to_update}").isNull(),
            F.when(F.col("cis20cd_from_lookup").isNotNull(), F.col("cis20cd_from_lookup")).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop("cis20cd_from_lookup")


def clean_and_rename(df: DataFrame, variable_name_map: dict):
    for old_name, new_name in variable_name_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    df.show()
    df = df.withColumn("country_name_12", F.lower(F.col("country_name_12")))
    drop_columns = [col for col in df.columns if "OLD" in col]
    return df.drop(*drop_columns)


def assign_sample_new_previous(df: DataFrame, colum_name_to_assign: str, date_column: str, batch_colum: str):
    window = Window.partitionBy(date_column).orderBy(date_column, F.desc(batch_colum))
    df = df.withColumn(colum_name_to_assign, F.when(F.row_number().over(window) == 1, "new").otherwise("previous"))
    return df


def count_distinct_in_filtered_df(
    df: DataFrame,
    column_name_to_assign: str,
    column_to_count: str,
    filter_positive: Any,
    filter_negative: Any,
    window: Window,
):
    eligible_df = df.filter(filter_positive)
    eligible_df = eligible_df.withColumn(column_name_to_assign, F.approx_count_distinct(column_to_count).over(window))
    ineligible_df = df.filter(filter_negative)
    ineligible_df = ineligible_df.withColumn(column_name_to_assign, 0)
    print("ineligable df: ")
    ineligible_df.show()
    df = eligible_df.unionByName(ineligible_df)
    return df


def assign_tranche_factor(df: DataFrame, column_name_to_assign: str, barcode_column: str, group_by_columns: List[str]):
    df.toPandas().to_csv("test_output4.csv", index=False)
    df = df.withColumn("tranche_eligible_households", F.when(F.col(barcode_column).isNotNull(), "Yes").otherwise("No"))
    window = Window.partitionBy(*group_by_columns)
    df = count_distinct_in_filtered_df(
        df,
        "number_eligible_households_tranche_bystrata_enrolment",
        barcode_column,
        F.col("tranche_eligible_households") == "Yes",
        F.col("tranche_eligible_households") == "No",
        window,
    )
    df = count_distinct_in_filtered_df(
        df,
        "number_sampled_households_tranche_bystrata_enrolment",
        barcode_column,
        ((F.col("tranche_eligible_households") == "Yes") & (F.col("tranche") == df.agg({"tranche": "max"}))),
        ((F.col("tranche_eligible_households") == "No") & (F.col("tranche") == df.agg({"tranche": "max"}))),
        window,
    )

    df.toPandas().to_csv("test_output4.csv", index=False)


resource_paths = {
    "old": {
        "path": r"C:\code\cis_households\old_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,tranche,sample,sample_direct,date _sample_created ,batch_number,file_name,hh_dweight_swab,hh_dweight_atb,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "new": {
        "path": r"C:\code\cis_households\new_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,sample,sample_direct,date _sample_created ,batch_number,file_name,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "lsoa_lookup": {"path": r"C:\code\cis_households\lookup.csv", "header": "pcd,ctry,lsoa11"},
    "cis20_lookup": {"path": r"C:\code\cis_households\cis20lookup.csv", "header": "LSOA11CD,LSOA11NM,CIS20CD,RGN19CD"},
    "tranche": {
        "path": r"C:\code\cis_households\tranche.csv",
        "header": "enrolement_date,UAC,lsoa_11,cis20cd,ctry12,ctry_name12,tranche",
    },
}
load_auxillary_data(resource_paths=resource_paths)
