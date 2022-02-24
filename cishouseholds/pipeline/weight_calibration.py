import contextlib
import os

import rpy2.robjects as robjects
import yaml
from pyspark.sql import functions as F
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr

from cishouseholds.pipeline.config import get_hdfs_config
from cishouseholds.pipeline.ETL_scripts import extract_input_data
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.weights.extract import prepare_auxillary_data
from cishouseholds.weights.population_projections import proccess_population_projection_df
from cishouseholds.weights.pre_calibration import pre_calibration_high_level
from cishouseholds.weights.weights import generate_weights

with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
    # silences import into text
    regenesees = importr(
        "ReGenesees",
        # Fix conflicting method names in R
        robject_translations={
            "dim.GVF.db": "dim_GVF_db_",
            "print.GVF.db": "print_GVF_db_",
            "str.GVF.db": "str_GVF_db_",
        },
    )


@register_pipeline_stage("sample_file_ETL")
def sample_file_ETL(
    address_lookup,
    cis_lookup,
    country_lookup,
    postcode_lookup,
    new_sample_file,
    tranche,
    table_or_path,
    old_sample_file,
    design_weight_table,
):
    files = {
        "address_lookup": address_lookup,
        "cis_lookup": cis_lookup,
        "country_lookup": country_lookup,
        "postcode_lookup": postcode_lookup,
        "new_sample_file": new_sample_file,
        "tranche": tranche,
    }
    dfs = extract_df_list(files, old_sample_file, table_or_path)
    dfs = prepare_auxillary_data(dfs)
    design_weights = generate_weights(dfs)
    update_table(design_weights, design_weight_table, mode_overide="overwrite")


@register_pipeline_stage("calculate_individual_level_population_totals")
def population_projection(
    population_projection_previous: str,
    population_projection_current: str,
    month: int,
    year: int,
    aps_lookup: str,
    table_or_path: str,
    population_totals_table: str,
    population_projections_table: str,
):
    files = {
        "population_projection_current": population_projection_current,
        "aps_lookup": aps_lookup,
        "population_projection_previous": population_projection_previous,
    }
    dfs = extract_df_list(files, population_projection_previous, table_or_path)
    populations_for_calibration, population_projections = proccess_population_projection_df(
        dfs=dfs, month=month, year=year
    )
    update_table(populations_for_calibration, population_totals_table, mode_overide="overwrite")
    update_table(population_projections, population_projections_table, mode_overide="overwrite")


def extract_df_list(files, previous, check_table_or_path):
    dfs = {}
    for key, file in files.items():
        if file == previous and check_table_or_path == "table":
            continue
        else:
            dfs[key] = extract_input_data(file_paths=file, validation_schema=None, sep=",")

    if check_table_or_path == "table":
        dfs[previous] = extract_from_table(previous)

    return dfs


@register_pipeline_stage("pre_calibration")
def pre_calibration(
    design_weight_table,
    population_projections_table,
    survey_response_table,
    responses_pre_calibration_table,
    pre_calibration_config_path,
):
    pre_calibration_config = get_hdfs_config(pre_calibration_config_path)
    household_level_with_design_weights = extract_from_table(design_weight_table)
    population_by_country = extract_from_table(population_projections_table)

    survey_response = extract_from_table(survey_response_table)

    survey_response = survey_response.select(
        "ons_household_id",
        "participant_id",
        "sex",
        "age_at_visit",
        "ethnicity_white",
    )

    population_by_country = population_by_country.select(
        # "region_code",
        "country_name_12",
        "population_country_swab",
        "population_country_antibodies",
    ).distinct()

    df_for_calibration = pre_calibration_high_level(
        df_survey=survey_response,
        df_dweights=household_level_with_design_weights,
        df_country=population_by_country,
        pre_calibration_config=pre_calibration_config,
    )
    update_table(df_for_calibration, responses_pre_calibration_table, mode_overide="overwrite")


@register_pipeline_stage("weight_calibration")
def weight_calibration(
    population_totals_table: str,
    responses_pre_calibration_table: str,
    base_output_table_name: str,
    calibration_config_path: str,
):
    """
    Run weight calibration for multiple datasets, as specified by the stage configuration.

    calibration_config_path
        path to YAML file containing a list of dictionaries with keys:
            dataset_name: swab_evernever
            country: string country name in lower case
            bounds: list of lists containing lower and upper bounds
            design_weight_column: string column name
            calibration_model_components: list of string column names

    """
    spark_session = get_or_create_spark_session()

    with open(calibration_config_path, "r") as config_file:
        calibration_config = yaml.load(config_file, Loader=yaml.FullLoader)
    population_totals_df = extract_from_table(population_totals_table)
    full_response_level_df = extract_from_table(responses_pre_calibration_table)

    for dataset_options in calibration_config:
        population_totals_subset = (
            population_totals_df.where(F.col("dataset_name") == dataset_options["dataset_name"])
            .drop("dataset_name")
            .toPandas()
            .transpose()
        )
        population_totals_subset = population_totals_subset.rename(columns=population_totals_subset.iloc[0]).drop(
            population_totals_subset.index[0]
        )
        assert population_totals_subset.shape[0] != 0, "Population totals subset is empty."
        population_totals_vector = robjects.vectors.FloatVector(population_totals_subset.iloc[0].tolist())

        columns_to_select = (
            ["participant_id", "country_name_12"]
            + [dataset_options["design_weight_column"]]
            + dataset_options["calibration_model_components"]
        )
        responses_subset_df = (
            full_response_level_df.select(columns_to_select)
            .where(
                (F.col(dataset_options["subset_flag_column"]) == 1)
                & (F.col("country_name_12") == dataset_options["country"])
            )
            .toPandas()
        )
        assert responses_subset_df.shape[0] != 0, "Responses subset is empty."

        with localconverter(robjects.default_converter + pandas2ri.converter):
            # population_totals_subset_r_df = robjects.conversion.py2rpy(population_totals_subset)
            responses_subset_df_r_df = robjects.conversion.py2rpy(responses_subset_df)

        sample_design = regenesees.e_svydesign(
            dat=responses_subset_df_r_df,
            ids=robjects.Formula("~participant_id"),
            weights=robjects.Formula(f"~{dataset_options['design_weight_column']}"),
        )
        for bounds in dataset_options["bounds"]:
            responses_subset_df_r_df = assign_calibration_weight(
                responses_subset_df_r_df,
                "calibration_weight",
                population_totals_vector,
                sample_design,
                dataset_options["calibration_model_components"],
                bounds,
            )
            responses_subset_df_r_df = assign_calibration_factors(
                responses_subset_df_r_df,
                "calibration_factors",
                "calibration_weight",
                dataset_options["design_weight_column"],
            )

            with localconverter(robjects.default_converter + pandas2ri.converter):
                calibrated_pandas_df = robjects.conversion.rpy2rp(responses_subset_df_r_df)

            calibrated_df = spark_session.createDataFrame(calibrated_pandas_df)
            update_table(
                calibrated_df,
                base_output_table_name
                + "_"
                + dataset_options["dataset_name"]
                + "_"
                + dataset_options["country_name_12"]
                + "_"
                + bounds[0]
                + "-"
                + bounds[1],
                mode_overide="overwrite",
            )


def convert_columns_to_r_factors(df: robjects.DataFrame, columns_to_covert: list) -> robjects.DataFrame:
    """
    Convert columns of an R dataframe to factors.
    Returns the dataframe, though changes are likely made in-place on the original dataframe.
    """
    for column in columns_to_covert:
        column_index = df.colnames.index(column)
        df[column_index] = robjects.vectors.FactorVector(df[column_index])
    return df


def assign_calibration_weight(
    df: robjects.DataFrame,
    column_name_to_assign: str,
    population_totals: robjects.DataFrame,
    sample_design,
    calibration_model_components: list,
    bounds: tuple,
) -> robjects.DataFrame:
    """
    Assign calibration weights to the specified column.
    """
    df[column_name_to_assign] = regenesees.weights(
        regenesees.e_calibrate(
            sample_design,
            population_totals,
            calmodel=robjects.Formula("+".join(calibration_model_components)),
            calfun="linear",
            bounds=robjects.ListVector(bounds[0], bounds[1]),
            aggregate_stage=1,
        )
    )
    return df


def assign_calibration_factors(
    df: robjects.DataFrame, column_name_to_assign: str, calibration_weight_column: str, design_weight_column: str
) -> robjects.DataFrame:
    """Assign calibration factors to specified column."""
    df[column_name_to_assign] = df[calibration_weight_column] / df[design_weight_column]
    return df
