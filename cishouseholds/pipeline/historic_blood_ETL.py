from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql.session import SparkSession

from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import rename_column_names
from cishouseholds.edit import update_schema_names
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.blood_delta_ETL import transform_blood_delta
from cishouseholds.pipeline.input_variable_names import historic_blood_variable_name_map
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import historic_blood_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


@register_pipeline_stage("blood_delta_ETL")
def blood_delta_ETL(resource_path: str):
    df = extract_validate_transform_historic_blood(resource_path)
    update_table(df, "processed_blood_test_results")
    return df


def extract_validate_transform_historic_blood(resource_path: str):
    spark_session = get_or_create_spark_session()

    df = extract_blood_delta(spark_session, resource_path)
    df = rename_column_names(df, historic_blood_variable_name_map)
    df = convert_columns_to_timestamps(df, blood_datetime_map)

    _historic_blood_validation_schema = update_schema_names(
        historic_blood_validation_schema, historic_blood_variable_name_map
    )

    df = convert_columns_to_timestamps(df, blood_datetime_map)
    blood_datetime_map_list = list(chain(*list(blood_datetime_map.values())))
    _historic_blood_validation_schema = update_schema_types(
        _historic_blood_validation_schema, blood_datetime_map_list, {"type": "timestamp"}
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )
    df = validate_and_filter(df, _historic_blood_validation_schema, error_accumulator)
    df = transform_blood_delta(df)
    # df = prepare_for_union(df, historic_blood_ETL)
    return df


def extract_blood_delta(spark_session: SparkSession, resource_path: str):
    historic_blood_spark_schema = convert_cerberus_schema_to_pyspark(historic_blood_validation_schema)

    raw_historic_bloods_header = ",".join(historic_blood_validation_schema.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        resource_path,
        raw_historic_bloods_header,
        historic_blood_spark_schema,
    )
    return df
