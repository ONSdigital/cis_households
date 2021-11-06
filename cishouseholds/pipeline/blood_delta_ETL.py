from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_test_target
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import substring_column
from cishouseholds.extract import get_files_by_date
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import blood_validation_schema


@register_pipeline_stage("blood_delta_ETL")
def blood_delta_ETL(resource_path: str, latest_only: bool = False, start_date: str = None, end_date: str = None):
    file_path = get_files_by_date(resource_path, latest_only=latest_only, start_date=start_date, end_date=end_date)
    df = extract_validate_transform_input_data(
        file_path, blood_variable_name_map, blood_datetime_map, blood_validation_schema, transform_blood_delta
    )
    df = add_historical_fields(df)
    df = df.select(sorted(df.columns))
    update_table_and_log_source_files(df, "transformed_blood_test_data", "blood_test_source_file")
    return df


def transform_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    """
    df = assign_filename_column(df, "blood_test_source_file")
    df = assign_test_target(df, "antibody_test_target", "blood_test_source_file")
    df = substring_column(df, "antibody_test_plate_common_id", "antibody_test_plate_id", 5, 5)
    df = assign_unique_id_column(
        df=df,
        column_name_to_assign="unique_antibody_test_id",
        concat_columns=["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"],
    )
    return df


def add_historical_fields(df: DataFrame):
    """
    Add empty values for union with historical data. Also adds constant
    values for continuation with historical data.
    """
    historical_columns = {
        "siemens_antibody_test_result_classification": "string",
        "siemens_antibody_test_result_value": "float",
        "tdi_antibody_test_result_value": "float",
        "lims_id": "string",
        "plate_storage_method": "string",
    }
    for column, type in historical_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))
    if "antibody_assay_category" not in df.columns:
        df = assign_column_uniform_value(df, "antibody_assay_category", "Post 2021-03-01")
    return df
