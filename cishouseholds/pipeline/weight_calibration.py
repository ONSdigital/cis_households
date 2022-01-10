# import rpy2.robjects as robjects
# import yaml
# from pyspark.sql import functions as F
from cishouseholds.pipeline.ETL_scripts import extract_input_data
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.weights.population_projections import proccess_population_projection_df
from cishouseholds.weights.pre_calibration import pre_calibration_high_level
from cishouseholds.weights.weights import generate_weights

# from cishouseholds.pyspark_utils import get_or_create_spark_session

# from rpy2.robjects.conversion import localconverter
# from rpy2.robjects.packages import importr
# from cishouseholds.weights.pre_calibration import calibration_datasets

# regenesees = importr(
#     "ReGenesees",
#     # Fix conflicting method names in R
#     robject_translations={
#         "dim.GVF.db": "dim_GVF_db_",
#         "print.GVF.db": "print_GVF_db_",
#         "str.GVF.db": "str_GVF_db_",
#     },
# )


@register_pipeline_stage("sample_file_ETL")
def sample_file_ETL(**kwargs):
    # TODO: Read lookups and input data
    list_paths = [
        "address_lookup",
        "cis_lookup",
        "country_lookup",
        "postcode_lookup",
        "new_sample_file",
        "tranche",
    ]
    dfs = extract_df_list(list_paths, "old_sample_file", **kwargs)
    design_weights = generate_weights(dfs)
    update_table(design_weights, "participant_geographies_design_weights", mode_overide="overwrite")


@register_pipeline_stage("population_projection")
def population_projection(**kwargs):
    list_paths = ["population_projection_current", "aps_lookup"]
    dfs = extract_df_list(list_paths, "population_projection_previous", **kwargs)
    (
        population_projections,
        populations_for_calibration,
    ) = proccess_population_projection_df(dfs=dfs, month=kwargs["month"])
    update_table(populations_for_calibration, "populations_for_calibration_table", mode_overide="overwrite")
    update_table(population_projections, "population_projections_table", mode_overide="overwrite")


def extract_df_list(list_paths, previous, **kwargs):
    check_table_or_path = kwargs["table_or_path"]
    if check_table_or_path == "path":
        list_paths.append(previous)

    elif check_table_or_path == "table":
        # TODO: Read "population_projection_current", "aps_lookup" from CSV
        population_projection_previous = extract_from_table(kwargs[previous])

    dfs = {}
    for key in list_paths:
        dfs[key] = extract_input_data(file_paths=kwargs[key], validation_schema=None, sep=",")

    if check_table_or_path == "table":
        dfs[previous] = population_projection_previous

    return dfs


@register_pipeline_stage("pre_calibration")
def pre_calibration(**kwargs):
    participant_level_with_design_weights = extract_from_table(kwargs["design_weight_table"])
    population_by_country = extract_from_table(kwargs["population_country_table"])
    survey_response = extract_from_table(kwargs["survey_response_table"])

    import pdb

    pdb.set_trace()

    survey_response = survey_response.select(
        "ons_household_id",
        "participant_id",
        "sex",
        "age_at_visit",
        "ethnicity_white",
        "region_code",
    )

    # NEEDED:
    # participant_level_with_design_weights.select(
    #     'index_multiple_deprivation',
    #     'country_name_12',
    #     'index_multiple_deprivation',
    #     'sample_addressbase_indicator',
    #     'cis_area_code_20',
    # )

    population_by_country = population_by_country.select(
        "country_code_#",
        "country_name_#",
        "population_country_swab",
        "population_country_antibodies",
    ).distinct()

    df_for_calibration = pre_calibration_high_level(
        df_survey=survey_response,
        df_dweights=participant_level_with_design_weights,
        df_country=population_by_country,
    )
    update_table(df_for_calibration, "responses_pre_calibration_table", mode_overide="overwrite")


# @register_pipeline_stage("weight_calibration")
# def weight_calibration(
#     population_totals_table: str,
#     responses_pre_calibration_table: str,
#     base_output_table_name: str,
#     calibration_config_path: str,
# ):
#     """
#     Run weight calibration for multiple datasets, as specified by the stage configuration.

#     calibration_config_path
#         path to YAML file containing a list of dictionaries with keys:
#             dataset_name: swab_evernever
#             country: string country name in title case
#             bounds: list of lists containing lower and upper bounds
#             design_weight_column: string column name
#             calibration_model_components: list of string column names

#     """
#     spark_session = get_or_create_spark_session()

#     with open(calibration_config_path, "r") as config_file:
#         calibration_config = yaml.load(config_file, Loader=yaml.FullLoader)
#     population_totals_df = extract_from_table(population_totals_table)
#     full_response_level_df = extract_from_table(responses_pre_calibration_table)

#     for dataset_options in calibration_config:
#         population_totals_subset = (
#             population_totals_df.where(F.col("dataset_name") == dataset_options["dataset_name"])
#             .drop("dataset_name")
#             .toPandas()
#             .transpose()
#         )

#         columns_to_select = calibration_datasets[dataset_options["dataset_name"]]["columns_to_select"]
#         responses_subset_df = (
#             full_response_level_df.where(
#                 (F.col(calibration_datasets[dataset_options["dataset_name"]]["subset_flag_column"]) == 1)
#                 & (F.col("country") == dataset_options["country"])
#             )
#             .select(columns_to_select)
#             .toPandas()
#         )

#         with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
#             population_totals_subset_r_df = robjects.conversion.py2rpy(population_totals_subset)
#             responses_subset_df_r_df = robjects.conversion.py2rpy(responses_subset_df)

#         sample_design = regenesees.e_svydesign(
#             dat=responses_subset_df_r_df,
#             ids=robjects.Formula("~participant_id"),
#             weights=robjects.Formula(f"~{dataset_options['design_weight_column']}"),
#         )
#         for bounds in dataset_options["bounds"]:
#             responses_subset_df_r_df = assign_calibration_weight(
#                 responses_subset_df_r_df,
#                 "calibration_weight",
#                 population_totals_subset_r_df,
#                 sample_design,
#                 dataset_options["calibration_model_components"],
#                 bounds,
#             )
#             responses_subset_df_r_df = assign_calibration_factors(
#                 responses_subset_df_r_df,
#                 "calibration_factors",
#                 "calibration_weight",
#                 dataset_options["design_weight_column"],
#             )

#         with localconverter(robjects.default_converter + robjects.pandas2ri.converter):
#             calibrated_pandas_df = robjects.conversion.rpy2rp(responses_subset_df_r_df)

#         calibrated_df = spark_session.createDataFrame(calibrated_pandas_df)
#         update_table(
#             calibrated_df,
#             base_output_table_name
#             + "_"
#             + dataset_options["dataset_name"]
#             + "_"
#             + dataset_options["country"]
#             + "_"
#             + bounds[0]
#             + "-"
#             + bounds[1],
#             mode_overide="overwrite",
#         )


# def convert_columns_to_r_factors(df: robjects.DataFrame, columns_to_covert: list) -> robjects.DataFrame:
#     """
#     Convert columns of an R dataframe to factors.
#     Returns the dataframe, though changes are likely made in-place on the original dataframe.
#     """
#     for column in columns_to_covert:
#         column_index = df.colnames.index(column)
#         df[column_index] = robjects.vectors.FactorVector(df[column_index])
#     return df


# def assign_calibration_weight(
#     df: robjects.DataFrame,
#     column_name_to_assign: str,
#     population_totals: robjects.DataFrame,
#     sample_design,
#     calibration_model_components: list,
#     bounds: tuple,
# ) -> robjects.DataFrame:
#     """
#     Assign calibration weights to the specified column.
#     """
#     df[column_name_to_assign] = regenesees.weights(
#         regenesees.e_calibrate(
#             sample_design,
#             population_totals,
#             calmodel=robjects.Formula("+".join(calibration_model_components)),
#             calfun="linear",
#             bounds=robjects.ListVector(bounds[0], bounds[1]),
#             aggregate_stage=1,
#         )
#     )
#     return df


# def assign_calibration_factors(
#     df: robjects.DataFrame, column_name_to_assign: str, calibration_weight_column: str, design_weight_column: str
# ) -> robjects.DataFrame:
#     """Assign calibration factors to specified column."""
#     df[column_name_to_assign] = df[calibration_weight_column] / df[design_weight_column]
#     return df
