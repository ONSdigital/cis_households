from pyspark.accumulators import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import Schema
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import derive_ctpattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pyspark_utils import convert_cerberus_schema_to_pyspark
from cishouseholds.pyspark_utils import create_spark_session
from cishouseholds.validate import validate_and_filter
from cishouseholds.validate import validate_csv_fields
from cishouseholds.validate import validate_csv_header


def read_csv_to_pyspark_df(
    spark_session: SparkSession, csv_file_path: str, raw_header_row: str, schema: Schema, **kwargs
) -> DataFrame:
    """
    Validate and read a csv file into a PySpark DataFrame.

    Parameters
    ----------
    csv_file_path
        file to read to dataframe
    raw_header_row
        expected first line of file
    schema
        schema to use for returned dataframe, including desired column names

    Takes keyword arguments from ``spark.read.csv``, for example ``timestampFormat="yyyy-MM-dd HH:mm:ss 'UTC'"``.
    """
    validate_csv_header(csv_file_path, raw_header_row)
    validate_csv_fields(csv_file_path)

    return spark_session.read.csv(csv_file_path, header=True, schema=schema, **kwargs)


def swab_delta_ETL(delta_file_path: str):
    spark_session = create_spark_session()
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
    df = clean_swab_delta(df)
    df = validate_and_filter(spark_session, df, swab_validation_schema, error_accumulator)
    df = transform_swab_delta(spark_session, df)
    df = load_swab_delta(spark_session, df)


def clean_swab_delta(df: DataFrame) -> DataFrame:
    """Drop unused data from swab delta."""
    df = df.drop("test_kit")
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
    df = assign_column_convert_to_date(df, "result_mk_date", "Date Tested")
    df = derive_ctpattern(df, ["CH1-Cq", "CH2-Cq", "CH3-Cq"], spark_session)
    df = mean_across_columns(df, "ct_mean", ["CH1-Cq", "CH2-Cq", "CH3-Cq"])
    df = assign_isin_list(df, "ctonetarget", "ctpattern", ["N only", "OR only", "S only"])

    return df


def load_swab_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
