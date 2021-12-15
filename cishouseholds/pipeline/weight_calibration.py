import rpy2.robjects as robjects
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr

from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage

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
def weight_calibration(calibration_config):
    """
    Run weight calibration for multiple datasets, as specified by the stage configuration.
    """
    population_totals_df = extract_from_table(calibration_config["population_totals_table_name"]).toPandas()
    with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
        population_totals_df = robjects.conversion.py2rpy(population_totals_df)

    for dataset_options in calibration_config["dataset_options"]:
        pd_df = extract_from_table(dataset_options["input_table_name"]).toPandas()

        with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
            r_df = robjects.conversion.py2rpy(pd_df)

        sample_design = regenesees.e_svydesign(
            dat=r_df,
            ids=robjects.Formula("~participant_id"),
            weights=robjects.Formula(f"~{dataset_options['design_weight_column']}"),
        )
        for bounds in dataset_options["bounds"]:
            r_df = assign_calibration_weight(
                r_df,
                "calibration_weight",
                population_totals_df,
                sample_design,
                dataset_options["calibration_model_components"],
                bounds,
            )
            r_df = assign_calibration_factors(r_df, "calibration_weight", dataset_options["design_weight_column"])

        # TODO: Write output and summary


def convert_columns_to_r_factors(df, columns_to_covert: list):
    """
    Convert columns of an R dataframe to factors.
    Returns the dataframe, though changes are likely made in-place on the original dataframe.
    """
    for column in columns_to_covert:
        column_index = df.colnames.index(column)
        df[column_index] = robjects.vectors.FactorVector(df[column_index])
    return df


def assign_calibration_weight(
    df, column_name_to_assign, population_totals, sample_design, calibration_model_components: list, bounds: tuple
):
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


def assign_calibration_factors(df, column_name_to_assign, calibration_weight_column, design_weight_column):
    """Assign calibration factors to specified column."""
    df[column_name_to_assign] = df[calibration_weight_column] / df[design_weight_column]
    return df
