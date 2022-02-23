import contextlib
import os

import rpy2.robjects as robjects
from rpy2.robjects.packages import importr

from cishouseholds.pipeline.ETL_scripts import extract_input_data
from cishouseholds.pipeline.load import extract_from_table

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
