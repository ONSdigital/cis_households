from itertools import chain
from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import substring_column
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import blood_variable_name_map
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import antibody_time_map
from cishouseholds.pipeline.validation_schema import blood_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter

# from cishouseholds.compare import prepare_for_union


@register_pipeline_stage("blood_delta_ETL")
def blood_delta_ETL(resource_path: str):
    df = extract_validate_transform_blood_delta(resource_path)
    update_table(df, "processed_blood_test_results")
    return df


def extract_validate_transform_blood_delta(resource_path: str):
    spark_session = get_or_create_spark_session()

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )
    df = extract_blood_delta(spark_session, resource_path)
    df = convert_columns_to_timestamps(df, antibody_time_map)
    antibody_time_map_list = list(chain(*list(antibody_time_map.values())))
    _blood_validation_schema = update_schema_types(
        blood_validation_schema, antibody_time_map_list, {"type": "timestamp"}
    )

    df = validate_and_filter(df, _blood_validation_schema, error_accumulator)
    df = transform_blood_delta(df)
    # df = prepare_for_union(df, None)
    # df = load_bloods_delta(df)
    return df


def extract_blood_delta(spark_session: SparkSession, resource_path: str):
    bloods_spark_schema = convert_cerberus_schema_to_pyspark(blood_validation_schema)

    raw_bloods_delta_header = ",".join(blood_variable_name_map.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        resource_path,
        raw_bloods_delta_header,
        bloods_spark_schema,
    )
    return df


def transform_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    D1: substring_column
    D11: assign_column_uniform_value

    Parameters
    ----------
    df: pyspark.sql.DataFrame

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    df = substring_column(df, "plate", "antibody_test_plate_id", 5, 5)
    df = assign_column_uniform_value(df, "assay_category", 1)

    return df
