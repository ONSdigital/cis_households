from pyspark.sql import DataFrame

from cishouseholds.derive import assign_filename_column
from cishouseholds.extract import get_files_to_be_processed
from cishouseholds.pipeline.ETL_scripts import extract_validate_transform_input_data
from cishouseholds.pipeline.input_variable_names import unassayed_bloods_variable_name_map
from cishouseholds.pipeline.load import update_table_and_log_source_files
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage
from cishouseholds.pipeline.timestamp_map import blood_datetime_map
from cishouseholds.pipeline.validation_schema import unassayed_blood_validation_schema

# from cishouseholds.derive import assign_column_uniform_value
# from cishouseholds.derive import assign_substring
# from cishouseholds.derive import assign_test_target
# from cishouseholds.derive import assign_unique_id_column


@register_pipeline_stage("unassayed_blood_ETL")
def blood_delta_ETL(**kwargs):
    file_path_list = get_files_to_be_processed(**kwargs)
    if file_path_list:
        df = extract_validate_transform_input_data(
            file_path_list,
            unassayed_bloods_variable_name_map,
            blood_datetime_map,
            unassayed_blood_validation_schema,
            transform_unassayed_blood,
        )
        df = df.select(sorted(df.columns))
        update_table_and_log_source_files(df, "unassayed_blood_test_data", "unassayed_blood_source_file")


def transform_unassayed_blood(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for unassayed blood.
    """
    df = assign_filename_column(df, "unassayed_blood_source_file")
    # df = assign_test_target(df, "antibody_test_target", "unassayed_blood_source_file")
    # df = assign_unique_id_column(
    #    df=df,
    #    column_name_to_assign="unique_antibody_test_id",
    #    concat_columns=["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"],
    # )
    return df
