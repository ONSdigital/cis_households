from itertools import chain

from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import derive_cq_pattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.edit import convert_columns_to_timestamps
from cishouseholds.edit import update_schema_types
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.load import update_table
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import swab_time_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


@register_pipeline_stage("swab_delta_ETL")
def swab_delta_ETL(resource_path: str):
    """
    End to end processing of a swab delta CSV file.
    """
    spark_session = get_or_create_spark_session()
    swab_spark_schema = convert_cerberus_schema_to_pyspark(swab_validation_schema)

    raw_swab_delta_header = ",".join(swab_variable_name_map.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        resource_path,
        raw_swab_delta_header,
        swab_spark_schema,
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = convert_columns_to_timestamps(df, swab_time_map)
    swab_time_map_list = list(chain(*list(swab_time_map.values())))
    _swab_validation_schema = update_schema_types(swab_validation_schema, swab_time_map_list, {"type": "timestamp"})
    df = validate_and_filter(df, _swab_validation_schema, error_accumulator)
    df = transform_swab_delta(spark_session, df)
    update_table(df, "processed_swab_test_results")
    return df


def transform_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """
    Tranform swab delta - derive new fields that do not depend on merging with survey responses.

    """
    df = assign_column_to_date_string(df, "pcr_date", "pcr_datetime")
    df = derive_cq_pattern(
        df, ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"], spark_session
    )
    df = mean_across_columns(
        df, "mean_pcr_cq_value", ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"]
    )
    df = assign_isin_list(df, "one_positive_pcr_target_only", "cq_pattern", ["N only", "OR only", "S only"])

    return df
