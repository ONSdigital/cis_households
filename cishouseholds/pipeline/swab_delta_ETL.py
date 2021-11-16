from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import derive_cq_pattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import swab_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import swab_datetime_map
from cishouseholds.pipeline.validation_schema import swab_validation_schema
from cishouseholds.pyspark_utils import get_or_create_spark_session


@register_pipeline_stage("swab_delta_ETL")
def swab_delta_ETL(**kwargs):
    """
    End to end processing of a swab delta CSV file.
    """
    file_path_list = get_files_to_be_processed(**kwargs)
    if file_path_list:
        df = extract_validate_transform_input_data(
            file_path_list, swab_variable_name_map, swab_datetime_map, swab_validation_schema, [transform_swab_delta]
        )
        update_table_and_log_source_files(df, "transformed_swab_test_data", "swab_test_source_file")


def transform_swab_delta(df: DataFrame) -> DataFrame:
    """
    Transform swab delta - derive new fields that do not depend on merging with survey responses.
    """
    spark_session = get_or_create_spark_session()
    df = assign_filename_column(df, "swab_test_source_file")
    df = assign_unique_id_column(df, "unique_pcr_test_id", ["swab_sample_barcode", "pcr_datetime"])

    df = assign_column_to_date_string(df, "pcr_date", "pcr_datetime")
    df = derive_cq_pattern(
        df, ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"], spark_session
    )
    df = mean_across_columns(
        df, "mean_pcr_cq_value", ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"]
    )
    df = assign_isin_list(df, "one_positive_pcr_target_only", "cq_pattern", ["N only", "OR only", "S only"])
    return df
