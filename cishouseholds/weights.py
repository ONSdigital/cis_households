import re
from typing import Any
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_proportion_column
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pyspark_utils import get_or_create_spark_session

spark_session = get_or_create_spark_session()


lookup_variable_name_maps = {
    "address_lookup": {"uprn": "unique_property_reference_code", "postcode": "postcode"},
    "nspl_lookup": {"pcd": "postcode", "lsoa11": "lower_super_output_area_code_11", "ctry": "country_code_12"},
    "cis20cd_lookup": {"LSOA11CD": "lower_super_output_area_code_11", "CIS20CD": "cis_area_code_20"},
    "country_lookup": {"CTRY20CD": "country_code_12", "CTRY20NM": "country_name_12"},
    "old_new": {
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
    },
    "tranche": {"UAC": "ons_household_id"},
    "population_projection_previous_population_projection_current": {
        "laua": "local_authority_unitary_authority_code",
        "rgn": "region_code",
        "ctry": "country_code_#",
        "ctry_name": "country_name_#",
    },
}


def null_to_value(df: DataFrame, column_name_to_update: str, value: int = 0):
    return df.withColumn(
        column_name_to_update,
        F.when((F.col(column_name_to_update).isNull()) | (F.isnan(F.col(column_name_to_update))), value).otherwise(
            F.col(column_name_to_update)
        ),
    )


def reformat_table(df: DataFrame, m_f_columns: List[str]):
    """
    recast columns to rows within a population column
    and aggregate the names of these rows into m and f values for sex
    """
    cols = [col for col in df.columns if col not in m_f_columns]
    dfs = []
    for sex in ["m", "f"]:
        selected_columns = [col for col in m_f_columns if col[0] == sex]
        dfs.append(
            df.select(*cols, F.explode(F.array(*[F.col(c) for c in selected_columns])).alias("population")).withColumn(
                "sex", F.lit(sex)
            )
        )
    return dfs[0].unionByName(dfs[1])


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


def load_auxillary_data(resource_paths, specify=[]):
    """
    create dictionary of renamed dataframes after extracting from csv file
    """
    auxillary_dfs = {}
    for name, resource_path in resource_paths.items():
        if specify == [] or name in specify:
            auxillary_dfs[name] = read_csv_to_pyspark_df(
                spark_session, resource_path["path"], resource_path["header"], None
            )
    auxillary_dfs = rename_columns(auxillary_dfs)
    return auxillary_dfs


def generate_weights(resource_paths: dict):
    # initialise all dataframes in dictionary
    auxillary_dfs = load_auxillary_data(resource_paths=resource_paths)

    # initialise lookup dataframes
    household_info_df = household_design_weights(
        auxillary_dfs["address_lookup"],
        auxillary_dfs["nspl_lookup"],
        auxillary_dfs["cis20cd_lookup"],
        auxillary_dfs["country_lookup"],
    )

    df = get_matches(
        old_sample_df=auxillary_dfs["old"],
        new_sample_df=auxillary_dfs["new"],
        selection_columns=["lower_super_output_area_code_11", "cis_area_code_20"],
        barcode_column="ons_household_id",
    )

    # update and clean sample df's
    df = update_data(df, auxillary_dfs)
    df = clean_df(df)
    old_df = clean_df(auxillary_dfs["old"])
    df = union_multiple_tables(tables=[df, old_df])

    # transform sample files
    df = assign_sample_new_previous(df, "sample_new_previous", "date _sample_created", "batch_number")
    df = df.join(auxillary_dfs["tranche"], on="ons_household_id", how="outer").drop("UAC")
    df = assign_tranche_factor(df, "tranche_factor", "ons_household_id", ["cis_area_code_20", "enrolement_date"])

    df = calculate_dweight_swabs(
        df=df,
        household_info_df=household_info_df,
        column_name_to_assign="raw_design_weights_swab",
        sample_type_column="sample_new_previous",
        group_by_columns=["cis_area_code_20", "sample_new_previous"],
        barcode_column="ons_household_id",
        previous_dweight_column="household_level_designweight_swab",
    )
    df = calculate_overall_dweights(df, "raw_design_weights_swab", ["cis_area_code_20", "sample_new_previous"])


def household_design_weights(
    df_address_base: DataFrame, df_nspl: DataFrame, df_cis20cd: DataFrame, df_county: DataFrame
) -> DataFrame:
    """
    Join address base and nspl (National Statistics Postcode Lookup) to then left inner join
    lsoa (Lower Level Output Area) to get household count.
    df_address_base
        Dataframe with address base file
    df_nspl
        Dataframe linking postcodes and lower level output area.
    df_cis20cd
        Dataframe with cis20cd and interim id.
    """

    df = df_address_base.join(df_nspl, on="postcode", how="left").withColumn(
        "postcode", F.regexp_replace(F.col("postcode"), " ", "")
    )
    df = df.join(df_cis20cd, on="lower_super_output_area_code_11", how="left")
    df = df.join(df_county, on="country_code_12")

    area_window = Window.partitionBy("cis_area_code_20")
    df = df.withColumn(
        "number_of_households_population_by_cis",
        F.approx_count_distinct("unique_property_reference_code").over(area_window),
    )

    country_window = Window.partitionBy("country_code_12")
    df = df.withColumn(
        "number_of_households_population_by_country",
        F.approx_count_distinct("unique_property_reference_code").over(country_window),
    )
    return df


def get_matches(old_sample_df: DataFrame, new_sample_df: DataFrame, selection_columns: List[str], barcode_column: str):
    """
    assign column to denote whether the data of a given set of columns (selection_columns) matches
    between old and new sample df's (old_sample_df) (new_sample_df)
    """
    select_df = old_sample_df.select(barcode_column, *selection_columns)
    for col in select_df.columns:
        if col != barcode_column:
            select_df = select_df.withColumnRenamed(col, col + "_OLD")
    joined_df = new_sample_df.join(select_df, on=barcode_column, how="left")
    for col in selection_columns:
        joined_df = joined_df.withColumn(f"MATCHED_{col}", F.when(F.col(col) == F.col(f"{col}_OLD"), 1).otherwise(None))
    return joined_df


def update_data(df: DataFrame, auxillary_dfs: dict):
    """
    wrapper function for calling update column
    """
    df = update_column(
        df, auxillary_dfs["nspl_lookup"], "lower_super_output_area_code_11", ["country_code_12", "postcode"]
    )
    df = update_column(df, auxillary_dfs["cis20cd_lookup"], "cis_area_code_20", ["lower_super_output_area_code_11"])
    drop_columns = [col for col in df.columns if "MATCHED" in col]
    return df.drop(*drop_columns)


def update_column(df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]):
    """
    Assign column (column_name_to_update) new value from lookup dataframe (lookup_df) if the value does not match
    its counterpart in the old dataframe
    """
    lookup_df = lookup_df.withColumnRenamed(column_name_to_update, f"{column_name_to_update}_from_lookup")
    df = df.join(lookup_df, on=[*join_on_columns], how="left")
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(f"MATCHED_{column_name_to_update}").isNull(),
            F.when(
                F.col(f"{column_name_to_update}_from_lookup").isNotNull(), F.col(f"{column_name_to_update}_from_lookup")
            ).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop(f"{column_name_to_update}_from_lookup")


def clean_df(df: DataFrame):
    """
    Edit column values by applying predefined logic
    """
    df = df.withColumn("country_name_12", F.lower(F.col("country_name_12")))
    drop_columns = [col for col in df.columns if "OLD" in col]
    return df.drop(*drop_columns)


def assign_sample_new_previous(df: DataFrame, colum_name_to_assign: str, date_column: str, batch_colum: str):
    """
    Assign column by checking for highest batch number in most recent date where new is value if true
    and previous otherwise
    """
    window = Window.partitionBy(date_column).orderBy(date_column, F.desc(batch_colum))
    df = df.withColumn("DATE_REFERENCE", F.first(date_column).over(window))
    df = df.withColumn("BATCH_REFERENCE", F.first(batch_colum).over(window))
    df = df.withColumn(
        colum_name_to_assign,
        F.when(
            ((F.col(date_column) == F.col("DATE_REFERENCE")) & (F.col(batch_colum) == F.col("BATCH_REFERENCE"))), "new"
        ).otherwise("previous"),
    )
    return df.drop("DATE_REFERENCE", "BATCH_REFERENCE")


def count_distinct_in_filtered_df(
    df: DataFrame,
    column_name_to_assign: str,
    column_to_count: str,
    filter_positive: Any,
    window: Window,
):
    """
    Count distinct rows that meet a given condition over a predefined window (window)
    """
    eligible_df = df.filter(filter_positive)
    eligible_df = eligible_df.withColumn(column_name_to_assign, F.approx_count_distinct(column_to_count).over(window))
    eligible_df.toPandas().to_csv(f"eligable_{column_name_to_assign}.csv", index=False)
    ineligible_df = df.filter(~filter_positive).withColumn(column_name_to_assign, F.lit(0))
    ineligible_df.toPandas().to_csv(f"ineligable_{column_name_to_assign}.csv", index=False)
    df = eligible_df.unionByName(ineligible_df)
    return df


def assign_tranche_factor(df: DataFrame, column_name_to_assign: str, barcode_column: str, group_by_columns: List[str]):
    """
    Assign a variable tranche factor as the ratio between 2 derived columns
    (number_eligible_households_tranche_bystrata_enrolment),
    (number_sampled_households_tranche_bystrata_enrolment) when the household is eligible to be sampled and the tranche
    value is maximum within the predefined window (window)
    """
    df = df.withColumn("tranche_eligible_households", F.when(F.col(barcode_column).isNull(), "No").otherwise("Yes"))
    window = Window.partitionBy(*group_by_columns)
    df = count_distinct_in_filtered_df(
        df,
        "number_eligible_households_tranche_bystrata_enrolment",
        barcode_column,
        F.col("tranche_eligible_households") == "Yes",
        window,
    )
    filter_max_condition = (F.col("tranche_eligible_households") == "Yes") & (
        F.col("tranche") == df.agg({"tranche": "max"}).first()[0]
    )
    df = count_distinct_in_filtered_df(
        df,
        "number_sampled_households_tranche_bystrata_enrolment",
        barcode_column,
        filter_max_condition,
        window,
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            filter_max_condition,
            F.col("number_eligible_households_tranche_bystrata_enrolment")
            / F.col("number_sampled_households_tranche_bystrata_enrolment"),
        ).otherwise("missing"),
    )
    return df.drop(
        "number_eligible_households_tranche_bystrata_enrolment", "number_sampled_households_tranche_bystrata_enrolment"
    )


def calculate_dweight_swabs(
    df: DataFrame,
    household_info_df: DataFrame,
    column_name_to_assign: str,
    sample_type_column: str,
    group_by_columns: List[str],
    barcode_column: str,
    previous_dweight_column: str,
):
    """
    Assign design weight for swabs sample by applying the ratio between number_of_households_population_by_cis and
    number_eligible_household_sample when the sample type is "new"
    """
    window = Window.partitionBy(*group_by_columns)
    df = df.join(
        household_info_df.select(
            "number_of_households_population_by_cis", "number_of_households_population_by_country", "cis_area_code_20"
        ),
        on="cis_area_code_20",
        how="outer",
    )
    df = df.withColumn("number_eligible_household_sample", F.approx_count_distinct(barcode_column).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.col(sample_type_column) == "new",
            F.col("number_of_households_population_by_cis") / F.col("number_eligible_household_sample"),
        ).otherwise(F.col(previous_dweight_column)),
    )
    return df


def calculate_overall_dweights(df: DataFrame, design_weight_column: str, groupby_columns: List[str]) -> DataFrame:
    """
    Apply logic to derive overall design weights
    """
    window = Window.partitionBy(*groupby_columns)
    df = df.withColumn("sum_raw_design_weight_swab_cis", F.sum(design_weight_column).over(window))
    df = df.withColumn("standard_deviation_raw_design_weight_swab", F.stddev(design_weight_column).over(window))
    df = df.withColumn("mean_raw_design_weight_swab", F.mean(design_weight_column).over(window))

    df = df.withColumn(
        "coefficient_variation_design_weight_swab",
        (F.col("standard_deviation_raw_design_weight_swab") / F.col("mean_raw_design_weight_swab")),
    )
    df = df.withColumn("design_effect_weight_swab", 1 + F.pow(F.col("coefficient_variation_design_weight_swab"), 2))
    df = df.withColumn(
        "effective_sample_size_design_weight_swab",
        F.col("number_eligible_household_sample") / F.col("design_effect_weight_swab"),
    )
    df = null_to_value(df, "effective_sample_size_design_weight_swab")

    cis_window = Window.partitionBy(groupby_columns[0])
    df = df.withColumn(
        "sum_effective_sample_size_design_weight_swab",
        F.sum("effective_sample_size_design_weight_swab").over(cis_window),
    )
    df = df.withColumn(
        "combining_factor_design_weight_swab",
        F.col("effective_sample_size_design_weight_swab") / F.col("sum_effective_sample_size_design_weight_swab"),
    )
    df = df.withColumn(
        "combined_design_weight_swab", F.col("combining_factor_design_weight_swab") * F.col(design_weight_column)
    )

    df = df.withColumn("sum_combined_design_weight_swab", F.sum("combined_design_weight_swab").over(cis_window))
    df = df.withColumn(
        "scaling_factor_combined_design_weight_swab",
        F.col("number_of_households_population_by_cis") / F.col("sum_combined_design_weight_swab"),
    )
    df = df.withColumn(
        "scaled_design_weight_swab_nonadjusted",
        F.col("scaling_factor_combined_design_weight_swab") * F.col("combined_design_weight_swab"),
    )
    df = validate_design_weights(df, "validated_design_weights", cis_window)
    df.toPandas().to_csv("test_output4.csv", index=False)


def validate_design_weights(df: DataFrame, column_name_to_assign: str, window: Window):
    """
    Validate the derived design weights by checking 3 conditions are true:
    - design weights add to household population total
    - no design weights are negative
    - no design weights are missing
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.sum("scaled_design_weight_swab_nonadjusted").over(window)
            == F.col("number_of_households_population_by_cis"),
            "True",
        ).otherwise("False"),
    )  # check 1

    columns = [col for col in df.columns if "weight" in col and list(df.select(col).dtypes[0])[1] == "double"]

    for col in columns:
        df = null_to_value(df, col)  # update nulls to 0
        df = df.withColumn(
            column_name_to_assign,
            F.when(F.approx_count_distinct(col).over(window) != 1, "False").otherwise(F.col(column_name_to_assign)),
        )

    df = df.withColumn("LEAST", F.least(*columns))
    df = df.withColumn(
        column_name_to_assign, F.when(F.col("LEAST") <= 0, "False").otherwise(F.col(column_name_to_assign))
    )  # flag missing

    return df.drop("LEAST")


def carry_forward_dweights(df: DataFrame, scenario: str, window: Window, household_population_column: str):
    """
    Use scenario lookup to apply dependent function to carry forward design weights variable
    to current dataframe
    """
    scenario_carry_forward_lookup = {
        "A": "raw_design_weights_antibodies_ab",
        "B": "raw_design_weights_antibodies_ab",
        "C": "combined_design_weight_antibodies_c",
    }
    df = df.withColumn("carryforward_design_weight_antibodies", F.col(scenario_carry_forward_lookup[scenario]))
    df = df.withColumn(
        "sum_carryforward_design_weight_antibodies", F.sum("carryforward_design_weight_antibodies").over(window)
    )
    df = df.withColumn(
        "scalling_factor_carryforward_design_weight_antibodies",
        F.col(household_population_column) / F.col("sum_carryfoward_design_weight_antibodies"),
    )
    df = df.withColumn(
        "scaled_design_weight_antibodies_nonadjusted",
        F.col("scalling_factor_carryforward_design_weight_antibodies") * F.col("carryforward_design_weight_antibodies"),
    )


def proccess_population_projection_df(previous_projection_df: DataFrame, current_projection_df: DataFrame, month: int):
    """
    process and format population projections tables by reshaping new datafrmae and recalculating predicted values
    """
    r = re.compile(r"\w{1}\d{1,}")
    m_f_columns = [
        item for item in list(filter(r.match, current_projection_df.columns)) if item in previous_projection_df.columns
    ]

    selected_columns = [
        "local_authority_unitary_authority_code",
        "region_code",
        "country_code_#",
        "country_name_#",
        *m_f_columns,
    ]
    previous_projection_df = previous_projection_df.select(*selected_columns).withColumn(
        "id", F.monotonically_increasing_id()
    )
    current_projection_df = current_projection_df.select(*selected_columns).withColumn(
        "id", F.monotonically_increasing_id()
    )

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumnRenamed(col, f"{col}_new")
    current_projection_df = current_projection_df.join(
        previous_projection_df.select("id", *m_f_columns), on="id", how="left"
    )

    if month < 6:
        a = 6 - month
        b = 6 + month
    else:
        a = 18 - month
        b = month - 6

    for col in m_f_columns:
        current_projection_df = current_projection_df.withColumn(
            col, F.lit(1 / 12) * ((a * F.col(col)) + (b * F.col(f"{col}_new")))
        )
        current_projection_df = current_projection_df.drop(f"{col}_new")

    current_projection_df = reformat_table(current_projection_df, m_f_columns)


def update_values(df: DataFrame):
    maps = {
        "interim_region_code": {
            "E12000001": 1,
            "E12000002": 2,
            "E12000003": 3,
            "E12000004": 4,
            "E12000005": 5,
            "E12000006": 6,
            "E12000007": 7,
            "E12000008": 8,
            "E12000009": 9,
            "W99999999": 10,
            "S99999999": 11,
            "N99999999": 12,
        },
        "interim_sex": {"male": 1, "female": 2},
    }
    age_maps = {
        "age_group_swab": {
            2: 1,
            12: 2,
            17: 3,
            25: 4,
            35: 5,
            50: 6,
            70: 7,
        },
        "age_group_antibodies": {
            16: 1,
            25: 2,
            35: 3,
            50: 4,
            70: 5,
        },
    }
    df = df.withColumn("interim_region_code", F.col("region_code"))
    df = df.withColumn("interim_sex", F.col("sex"))

    for col, map in maps.items():
        df = update_column_values_from_map(df, col, map)

    for col, map in age_maps.items():
        df = assign_named_buckets(df, col, col, map)

    df = df.withColumn(
        "p1_for_swab_longcovid",
        F.when(
            F.col("country_name_#") == "England",
            (F.col("interim_region_code") - 1) * 14 + (F.col("interim_sex") - 1) * 7 + F.col("age_group_swab"),
        ).otherwise((F.col("interim_sex") - 1) * 7 + F.col("age_group_swab")),
    )
    df = df.withColumn(
        "p1_for_antibodies_evernever_engl",
        F.when(
            F.col("country_name_#") == "England",
            ((F.col("interim_region_code") - 1) * 10)
            + ((F.col("interim_sex") - 1) * 5)
            + F.col("age_group_antibodies"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_28daysto_engl",
        F.when(
            F.col("country_name_#") == "England",
            (F.col("interim_sex") - 1) * 5 + F.col("agegroup_antibodies"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_wales_scot_ni",
        F.when(F.col("country_name_#") == "England", None,).otherwise(
            F.col("country_name_#") == "England",
            (F.col("interim_sex") - 1) * 5 + F.col("agegroup_antibodies"),
        ),
    )
    df = df.withColumn(
        "p3_for_antibodies_28daysto_engl",
        F.when(
            (F.col("country_name_#") == "England") & (F.col("age_group_antibodies").isNull()),
            F.col("interim_region_code"),
        )
        .when(F.col("country_name_#") == "England", "missing")
        .otherwise(None),
    )


def calculate_population_totals(df: DataFrame, group_by_column: str, population_column: str):
    window = Window.partitionBy(group_by_column)
    df = df.withColumn(
        "population_country_swab",
        F.sum(F.when(F.col(population_column) >= 2, F.col(population_column)).otherwise(0)).over(window),
    )
    df = df.withColumn(
        "population_country_ antibodies",
        F.sum(F.when(F.col(population_column) >= 2, F.col(population_column)).otherwise(0)).over(window),
    )
    # df = assign_proportion_column(df, "percentage_white_ethnicity_country_over16", )
    df = df.withColumn(
        "p22_white_population_antibodies",
        F.col("population_country_antibodies") * F.col("percentage_white_ethnicity_country_over16"),
    )


# fmt: off
resource_paths = {
    "old": {
        "path": r"C:\code\cis_households\old_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,tranche,sample,sample_direct,date _sample_created ,batch_number,file_name,hh_dweight_swab,hh_dweight_atb,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "new": {
        "path": r"C:\code\cis_households\new_sample_file.csv",
        "header": "UAC,postcode,lsoa_11,cis20cd,ctry12,ctry_name12,sample,sample_direct,date _sample_created ,batch_number,file_name,rgn/gor9d,laua,oa11/ oac11,msoa11,ru11ind,imd",
    },
    "nspl_lookup": {"path": r"C:\code\cis_households\lookup.csv", "header": "pcd,ctry,lsoa11"},
    "cis20cd_lookup": {
        "path": r"C:\code\cis_households\cis20lookup.csv",
        "header": "LSOA11CD,LSOA11NM,CIS20CD,RGN19CD",
    },
    "address_lookup": {
        "path": r"C:\code\cis_households\Address_lookup.csv",
        "header": "uprn,town_name,postcode,ctry18nm,la_code,ew,address_type,council_tax,udprn,address_base_postal",
    },
    "country_lookup": {
        "path": r"C:\code\cis_households\country_lookup.csv",
        "header": "LAD20CD,LAD20NM,CTRY20CD,CTRY20NM",
    },
    "tranche": {
        "path": r"C:\code\cis_households\tranche.csv",
        "header": "enrolement_date,UAC,lsoa_11,cis20cd,ctry12,ctry_name12,tranche",
    },
    "population_projection_current": {
        "path": r"C:\code\cis_households\population_projectionC.csv",
        "header": "laua,rgn,ctry,ctry_name,surge,m0,m1,f0,f1",
    },
    "population_projection_previous": {
        "path": r"C:\code\cis_households\population_projectionP.csv",
        "header": "laua,rgn,ctry,ctry_name,surge,m0,m1,f0,f1",
    },
}
# fmt: on
# generate_weights(resource_paths=resource_paths)

dfs = load_auxillary_data(
    resource_paths=resource_paths, specify=["population_projection_current", "population_projection_previous"]
)

proccess_population_projection_df(
    previous_projection_df=dfs["population_projection_previous"],
    current_projection_df=dfs["population_projection_current"],
    month=7,
)
