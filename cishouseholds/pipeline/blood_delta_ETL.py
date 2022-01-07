from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_substring
from cishouseholds.derive import assign_test_target
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.edit import clean_barcode


def transform_blood_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for blood deltas.
    """
    df = assign_filename_column(df, "blood_test_source_file")
    df = assign_test_target(df, "antibody_test_target", "blood_test_source_file")
    df = assign_substring(
        df,
        column_name_to_assign="antibody_test_plate_common_id",
        column_to_substring="antibody_test_plate_id",
        start_position=5,
        substring_length=5,
    )
    df = assign_unique_id_column(
        df=df,
        column_name_to_assign="unique_antibody_test_id",
        concat_columns=["blood_sample_barcode", "antibody_test_plate_common_id", "antibody_test_well_id"],
    )
    df = clean_barcode(df=df, barcode_column="blood_sample_barcode")
    return df


def add_historical_fields(df: DataFrame):
    """
    Add empty values for union with historical data. Also adds constant
    values for continuation with historical data.
    """
    historical_columns = {
        "siemens_antibody_test_result_classification": "string",
        "siemens_antibody_test_result_value": "float",
        "antibody_test_tdi_result_value": "float",
        "lims_id": "string",
        "plate_storage_method": "string",
    }
    for column, type in historical_columns.items():
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None).cast(type))
    if "antibody_assay_category" not in df.columns:
        df = assign_column_uniform_value(df, "antibody_assay_category", "Post 2021-03-01")
    df = df.select(sorted(df.columns))
    return df
