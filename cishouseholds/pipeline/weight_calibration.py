import rpy2.robjects as robjects
import yaml
from pyspark.sql import functions as F
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr

from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.weights.pre_calibration import calibration_datasets

regenesees = importr(
    "ReGenesees",
    # Fix conflicting method names in R
    robject_translations={
        "dim.GVF.db": "dim_GVF_db_",
        "print.GVF.db": "print_GVF_db_",
        "str.GVF.db": "str_GVF_db_",
    },
)


@register_pipeline_stage("weight_calibration")
def weight_calibration(table_names: dict, calibration_config_path: str):
    """
    Run weight calibration for multiple datasets, as specified by the stage configuration.

    calibration_config_path
        path to YAML file containing a list of dictionaries with keys:
            dataset_name: swab_evernever
            country: string country name in title case
            bounds: list of lists containing lower and upper bounds
            design_weight_column: string column name
            calibration_model_components: list of string column names

    """
    spark_session = get_or_create_spark_session()

    with open(calibration_config_path, "r") as config_file:
        calibration_config = yaml.load(config_file, Loader=yaml.FullLoader)
    population_totals_df = extract_from_table(table_names["input"]["population_totals"])
    full_response_level_df = extract_from_table(table_names["input"]["survey_responses"])

    for dataset_options in calibration_config:
        population_totals_subset = (
            population_totals_df.where(F.col("dataset_name") == dataset_options["dataset_name"])
            .drop("dataset_name")
            .toPandas()
            .transpose()
        )

        columns_to_select = calibration_datasets[dataset_options["dataset_name"]]["columns_to_select"]
        responses_subset_df = (
            full_response_level_df.where(
                (F.col(calibration_datasets[dataset_options["dataset_name"]]["subset_flag_column"]) == 1)
                & (F.col("country") == dataset_options["country"])
            )
            .select(columns_to_select)
            .toPandas()
        )

        with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
            population_totals_subset_r_df = robjects.conversion.py2rpy(population_totals_subset)
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
                population_totals_subset_r_df,
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

        with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
            calibrated_pandas_df = robjects.conversion.rpy2rp(responses_subset_df_r_df)

        calibrated_df = spark_session.createDataFrame(calibrated_pandas_df)
        update_table(
            calibrated_df,
            table_names["output"]["base_output_table_name"]
            + "_"
            + dataset_options["dataset_name"]
            + "_"
            + dataset_options["country"]
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
