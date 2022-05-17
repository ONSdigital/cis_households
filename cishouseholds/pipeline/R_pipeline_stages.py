from cishouseholds.pipeline.config import get_secondary_config
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.weight_calibration import calibrate_weights
from cishouseholds.pipeline.weight_calibration import prepare_population_totals_vector
from cishouseholds.pipeline.weight_calibration import prepare_sample_design
from cishouseholds.pipeline.weight_calibration import subset_for_calibration
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("weight_calibration")
def weight_calibration(
    individual_level_populations_for_calibration_table: str,
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

    Parameters
    ----------
    individual_level_populations_for_calibration_table
        name of HIVE table containing populations for calibration
    responses_pre_calibration_table
        name of HIVE table containing responses from pre-calibration
    base_output_table_name
        base name to use for output tables, will be suffixed with dataset and country names to identify outputs
    calibration_config_path
        path to YAML calibration config file
    """
    spark_session = get_or_create_spark_session()

    calibration_config = get_secondary_config(calibration_config_path)
    population_totals_df = extract_from_table(individual_level_populations_for_calibration_table)
    full_response_level_df = extract_from_table(responses_pre_calibration_table)

    if calibration_config is not None:
        for dataset_options in calibration_config:
            population_totals_vector = prepare_population_totals_vector(
                population_totals_df, dataset_options["dataset_name"]
            )
            responses_subset_df_r_df = subset_for_calibration(
                full_response_level_df,
                dataset_options["design_weight_column"],
                dataset_options["calibration_model_components"],
                dataset_options["subset_flag_column"],
                dataset_options["country"],
            )
            sample_design = prepare_sample_design(responses_subset_df_r_df, dataset_options["design_weight_column"])

            for bounds in dataset_options["bounds"]:
                calibrated_pandas_df = calibrate_weights(
                    responses_subset_df_r_df,
                    population_totals_vector,
                    sample_design,
                    dataset_options["calibration_model_components"],
                    dataset_options["design_weight_column"],
                    bounds,
                )
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
                    write_mode="overwrite",
                )
