import pyspark.sql.functions as F
from pyspark.AccumulatorParam import AddingAccumulatorParam
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.derive import assign_column_convert_to_date
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import derive_ctpattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.validation_schema import swabs_validation_schema
from cishouseholds.pyspark_utils import create_spark_session
from cishouseholds.validate import validate_and_filter


def swabs_delta_ETL(delta_file_path: str):
    spark_session = create_spark_session()
    df = spark_session.read.csv(delta_file_path, header=True)
    error_accumulator = spark_session.sparkContext.accumulator(
        value=[], accum_param=AddingAccumulatorParam(zero_value=[])
    )

    df = clean_swabs_delta(spark_session, df)
    df = validate_and_filter(spark_session, df, swabs_validation_schema, error_accumulator)
    df = transform_swabs_delta(spark_session, df)
    df = load_swabs_delta(spark_session, df)


def clean_swabs_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """Clean column names, drop unused data and parse datetime fields on swab delta dataframe."""
    df = rename_column_names(df, swab_variable_name_map)
    df = df.drop("test_kit")
    df = df.withColumn("swab_pcr_test_date", F.to_timestamp("swab_pcr_test_date", "yyyy-MM-dd HH:mm:ss UTC"))


def transform_swabs_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    """
    Call functions to process input for swabs deltas.

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


def load_swabs_delta(spark_session: SparkSession, df: DataFrame) -> DataFrame:
    return df
