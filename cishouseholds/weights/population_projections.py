from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.weights.derive import assign_ethnicity_white
from cishouseholds.weights.derive import assign_population_projections
from cishouseholds.weights.derive import assign_white_proportion
from cishouseholds.weights.derive import derive_m_f_column_list
from cishouseholds.weights.edit import reformat_age_population_table
from cishouseholds.weights.edit import reformat_calibration_df_simple
from cishouseholds.weights.edit import update_population_values
from cishouseholds.weights.extract import prepare_auxillary_data


def proccess_population_projection_df(dfs: dict, month: int, year: int):
    """
    process and format population projections tables by reshaping new dataframe and recalculating predicted values
    """
    dfs = prepare_auxillary_data(dfs)
    previous_projection_df = dfs["population_projection_previous"]
    individual_level_populations_for_non_response_adjustment = dfs["population_projection_current"]

    m_f_columns = derive_m_f_column_list(df=individual_level_populations_for_non_response_adjustment)

    selected_columns = [
        "local_authority_unitary_authority_code",
        "region_code",
        "country_code_12",
        "country_name_12",
        *m_f_columns,
    ]
    previous_projection_df = previous_projection_df.select(*selected_columns).withColumn(
        "id", F.monotonically_increasing_id()
    )
    individual_level_populations_for_non_response_adjustment = (
        individual_level_populations_for_non_response_adjustment.select(*selected_columns).withColumn(
            "id", F.monotonically_increasing_id()
        )
    )

    individual_level_populations_for_non_response_adjustment = assign_population_projections(
        current_projection_df=individual_level_populations_for_non_response_adjustment,
        previous_projection_df=previous_projection_df,
        month=month,
        m_f_columns=m_f_columns,
    )

    individual_level_populations_for_non_response_adjustment = reformat_age_population_table(
        individual_level_populations_for_non_response_adjustment, m_f_columns
    )

    aps_lookup_df = assign_ethnicity_white(
        dfs["aps_lookup"],
        "ethnicity_white",
        "country_name",
        "ethnicity_aps_northen_ireland",
        "ethnicity_aps_engl_wales_scot",
    )
    aps_lookup_df = assign_white_proportion(
        aps_lookup_df,
        "percentage_white_ethnicity_country_over16",
        "ethnicity_white",
        "country_name",
        "person_level_weight_aps_18",
        "age",
    )

    individual_level_populations_for_non_response_adjustment = (
        individual_level_populations_for_non_response_adjustment.join(
            aps_lookup_df.select("country_name", "percentage_white_ethnicity_country_over16"),
            individual_level_populations_for_non_response_adjustment["country_name_12"]
            == dfs["aps_lookup"]["country_name"],
            how="left",
        )
    )
    individual_level_populations_for_non_response_adjustment = update_population_values(
        individual_level_populations_for_non_response_adjustment
    )
    individual_level_populations_for_non_response_adjustment = calculate_additional_population_columns(
        df=individual_level_populations_for_non_response_adjustment,
        country_name_column="country_name_12",
        region_code_column="interim_region_code",
        sex_column="interim_sex",
        age_group_swab_column="age_group_swab",
        age_group_antibody_column="age_group_antibodies",
    )
    individual_level_populations_for_non_response_adjustment = calculate_population_totals(
        df=individual_level_populations_for_non_response_adjustment,
        group_by_column="country_name_12",
        population_column="population",
        white_proportion_column="percentage_white_ethnicity_country_over16",
    )

    individual_level_populations_for_calibration = get_calibration_dfs(
        individual_level_populations_for_non_response_adjustment, "country_name_12", "age"
    )
    return individual_level_populations_for_calibration, individual_level_populations_for_non_response_adjustment


def calculate_additional_population_columns(
    df: DataFrame,
    country_name_column: str,
    region_code_column: str,
    sex_column: str,
    age_group_swab_column: str,
    age_group_antibody_column: str,
):
    df = df.withColumn(
        "p1_for_swab_longcovid",
        F.when(
            F.col(country_name_column) == "England",
            (F.col(region_code_column) - 1) * 14 + (F.col(sex_column) - 1) * 7 + F.col(age_group_swab_column),
        ).otherwise((F.col(sex_column) - 1) * 7 + F.col(age_group_swab_column)),
    )
    df = df.withColumn(
        "p1_for_antibodies_evernever_engl",
        F.when(
            F.col(country_name_column) == "England",
            ((F.col(region_code_column) - 1) * 10) + ((F.col(sex_column) - 1) * 5) + F.col(age_group_antibody_column),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_28daysto_engl",
        F.when(
            F.col(country_name_column) == "England",
            (F.col(sex_column) - 1) * 5 + F.col(age_group_antibody_column),
        ).otherwise(None),
    )
    df = df.withColumn(
        "p1_for_antibodies_wales_scot_ni",
        F.when(F.col(country_name_column) == "England", None).otherwise(
            (F.col(sex_column) - 1) * 5 + F.col(age_group_antibody_column),
        ),
    )
    df = df.withColumn(
        "p3_for_antibodies_28daysto_engl",
        F.when(
            (F.col(country_name_column) == "England") & (F.col(age_group_antibody_column).isNull()),
            F.col(region_code_column),
        )
        .when(F.col(country_name_column) == "England", "missing")
        .otherwise(None),
    )
    return df


def calculate_population_totals(
    df: DataFrame, group_by_column: str, population_column: str, white_proportion_column: str
):
    window = Window.partitionBy(group_by_column)
    df = df.withColumn(
        "population_country_swab",
        F.sum(F.when(F.col(population_column) >= 2, F.col(population_column)).otherwise(0)).over(window),
    )
    df = df.withColumn(
        "population_country_antibodies",
        F.sum(F.when(F.col(population_column) >= 16, F.col(population_column)).otherwise(0)).over(window),
    )
    df = df.withColumn(
        "p22_white_population_antibodies",
        F.col("population_country_antibodies") * F.col(white_proportion_column),
    )
    return df


def calibarate_df(
    df: DataFrame,
    groupby_columns: List[str],
    country: str,
    country_column: str,
    min_age: int,
    age_column: str,
    additional_columns: List[str] = [],
):
    df = df.filter((F.col(country_column) == country) & (F.col(age_column) >= min_age))
    if "p1_for_swab_longcovid" in groupby_columns:
        df = df.filter((F.col("p1_for_swab_longcovid").isNotNull()))
    df = df.select(*groupby_columns, "population", *additional_columns)
    if df.count() > 0:
        df = reformat_calibration_df_simple(df=df, population_column="population", groupby_columns=groupby_columns)
        return df
    return None


def get_calibration_dfs(df: DataFrame, country_column: str, age_column: str):
    """
    create separate dataframes for population totals for specific groups
    reformatted to have population groups as column headings
    Parameters
    ----------
    df
    country_column
    age_column

    Notes
    -----
    requires at least 1 row for each country to be present
    """
    groupby_columns_set = [
        ["p1_for_swab_longcovid"],
        ["p1_for_swab_longcovid"],
        ["p1_for_swab_longcovid"],
        ["p1_for_swab_longcovid"],
        ["p1_for_antibodies_wales_scot_ni"],
        ["p1_for_antibodies_wales_scot_ni"],
        ["p1_for_antibodies_wales_scot_ni"],
        ["p1_for_antibodies_evernever_engl"],
        ["p1_for_antibodies_28daysto_engl", "p3_for_antibodies_28daysto_engl"],
    ]
    data_set_names = [
        "england_population_swab_longcovid",
        "wales_population_swab_longcovid",
        "scotland_population_swab_longcovid",
        "northen_ireland_population_swab_longcovid",
        "wales_population_any_antibodies",
        "scotland_population_any_antibodies",
        "northen_ireland_population_any_antibodies",
        "england_population_antibodies_evernever",
        "england_population_antibodies_28daysto",
    ]
    additional_columns_set = [
        [],
        [],
        [],
        [],
        ["p22_white_population_antibodies"],
        [],
        [],
        [],
        ["p22_white_population_antibodies"],
    ]
    min_ages = [2, 2, 2, 2, 16, 12, 16, 16, 16]
    countries = [
        "England",
        "Wales",
        "Scotland",
        "Northern Ireland",
        "Wales",
        "Scotland",
        "Northern Ireland",
        "England",
        "England",
    ]

    output_df = None
    for dataset_name, country, min_age, groupby_columns, additional_columns in zip(
        data_set_names, countries, min_ages, groupby_columns_set, additional_columns_set
    ):
        calibrated_df = calibarate_df(
            df=df,
            groupby_columns=groupby_columns,
            country=country,
            country_column=country_column,
            min_age=min_age,
            age_column=age_column,
            additional_columns=additional_columns,  # type: ignore
        )
        if calibrated_df is not None:
            calibrated_df = calibrated_df.withColumn("dataset_name", F.lit(dataset_name))
            if output_df is None:
                output_df = calibrated_df
            else:
                output_df = output_df.union(calibrated_df)

    return output_df
