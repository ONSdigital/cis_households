from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

from cishouseholds.extract import get_files_by_date
from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import historical_blood_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import historical_blood_validation_schema


@register_pipeline_stage("historical_blood_ETL")
def historical_blood_ETL(resource_path: str, latest_only: bool = False, start_date: str = None, end_date: str = None):
    file_path = get_files_by_date(resource_path, latest_only=latest_only, start_date=start_date, end_date=end_date)
    df = extract_validate_transform_input_data(
        file_path,
        historical_blood_variable_name_map,
        blood_datetime_map,
        historical_blood_validation_schema,
        transform_blood_delta,
    )
    df = add_fields(df)
    df = df.select(sorted(df.columns))
    update_table_and_log_source_files(df, "transformed_blood_test_data", "blood_test_source_file")
    return df


def add_fields(df: DataFrame):
    """Add fields that might be missing in example data."""
    new_columns = {
        "antibody_test_undiluted_result_value": "float",
        "antibody_test_bounded_result_value": "float",
    }
    for column, type in new_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))

    return df
