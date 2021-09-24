from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import derive_ctpattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.extract import read_csv_to_pyspark_df
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_and_filter


def swab_delta_ETL(delta_file_path: str):
    """
    End to end processing of a swab delta CSV file.
    """
    spark_session = get_or_create_spark_session()
    swab_spark_schema = convert_cerberus_schema_to_pyspark(swab_validation_schema)

    raw_swab_delta_header = ",".join(swab_variable_name_map.keys())
    df = read_csv_to_pyspark_df(
        spark_session,
        delta_file_path,
        raw_swab_delta_header,
        swab_spark_schema,
        timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'",
    )

    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = validate_and_filter(df, swab_validation_schema, error_accumulator)
    df = transform_swab_delta(spark_session, df)
    df = load_swab_delta(spark_session, df)
    return df


def transform_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """
    Call functions to process input for swab deltas.

    Parameters
    ----------
    df
    spark_session

    Notes
    -----
    Functions implemented:
        D13: assign_column_convert_to_date
        D7: derive_ctpattern
        D9: mean_across_columns
        D10: assign_isin_list
    """
    df = assign_column_convert_to_date(df, "pcr_date", "pcr_datetime")
    df = derive_ctpattern(df, ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"], spark_session)
    df = mean_across_columns(
        df, "mean_pcr_cq_value", ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"]
    )
    df = assign_isin_list(df, "ctonetarget", "ctpattern", ["N only", "OR only", "S only"])

    return df


def load_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
