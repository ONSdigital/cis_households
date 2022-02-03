import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.load import extract_from_table


def report(
    valid_df: DataFrame,
    invalid_df: DataFrame,
    unique_id_column: str,
    error_column: str,
    processed_file_table: str,
    invalid_files_table: str,
):
    processed_file_log = extract_from_table(processed_file_table)
    invalid_files_log = extract_from_table(invalid_files_table)
    processed_file_count = processed_file_log.count()
    invalid_files_count = invalid_files_log.count()
    num_valid_survey_responses = valid_df.count()
    num_invalid_survey_responses = invalid_df.count()
    valid_df_errors = valid_df.select(unique_id_column, error_column)
    invalid_df_errors = invalid_df.select(unique_id_column, error_column)

    valid_df_errors = valid_df_errors.withColumn("ERROR LIST", F.explode(error_column)).groupBy("ERROR LIST").count()
    invalid_df_errors = (
        invalid_df_errors.withColumn("ERROR LIST", F.explode(error_column)).groupBy("ERROR LIST").count()
    )


# report =SparkVal.report("unique_participant_response_id",processed_file_log,invalid_files_log)
