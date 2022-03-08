import contextlib
import os
from typing import List

import rpy2.robjects as robjects
from pyspark.sql import functions as F
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr


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


def prepare_population_totals_vector(population_totals_df, dataset_name: str):
    population_totals_subset = (
        population_totals_df.where(F.col("dataset_name") == dataset_name).drop("dataset_name").toPandas().transpose()
    )
    population_totals_subset = population_totals_subset.rename(columns=population_totals_subset.iloc[0]).drop(
        population_totals_subset.index[0]
    )
    assert population_totals_subset.shape[0] != 0, "Population totals subset is empty."
    population_totals_vector = robjects.vectors.FloatVector(population_totals_subset.iloc[0].tolist())
    return population_totals_vector


def subset_for_calibration(
    full_response_level_df,
    design_weight_column: str,
    calibration_model_components: List[str],
    subset_flag_column: str,
    country: str,
):
    columns_to_select = ["participant_id", "country_name_12"] + [design_weight_column] + calibration_model_components
    responses_subset_df = (
        full_response_level_df.select(columns_to_select)
        .where((F.col(subset_flag_column) == 1) & (F.col("country_name_12") == country))
        .toPandas()
    )
    assert responses_subset_df.shape[0] != 0, "Responses subset is empty."

    with localconverter(robjects.default_converter + pandas2ri.converter):
        responses_subset_df_r_df = robjects.conversion.py2rpy(responses_subset_df)
    return responses_subset_df_r_df


def prepare_sample_design(responses_subset_df_r_df, design_weight_column):
    sample_design = regenesees.e_svydesign(
        dat=responses_subset_df_r_df,
        ids=robjects.Formula("~participant_id"),
        weights=robjects.Formula(f"~{design_weight_column}"),
    )
    return sample_design


def calibrate_weights(
    responses_subset_df_r_df,
    population_totals_vector,
    sample_design,
    calibration_model_components: List[str],
    design_weight_column: str,
    bounds: tuple,
):
    """
    Carry out weight calibration for dataset.
    """
    responses_subset_df_r_df = assign_calibration_weight(
        responses_subset_df_r_df,
        "calibration_weight",
        population_totals_vector,
        sample_design,
        calibration_model_components,
        bounds,
    )
    responses_subset_df_r_df = assign_calibration_factors(
        responses_subset_df_r_df,
        "calibration_factors",
        "calibration_weight",
        design_weight_column,
    )

    with localconverter(robjects.default_converter + pandas2ri.converter):
        calibrated_pandas_df = robjects.conversion.rpy2rp(responses_subset_df_r_df)
    return calibrated_pandas_df
