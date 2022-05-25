import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.edit import clean_barcode_simple
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("digital_enrolment_invite_datetime"))
    df = transform_survey_responses_generic(df)
    df = clean_barcode_simple(df, "swab_sample_barcode_user_entered")
    df = clean_barcode_simple(df, "blood_sample_barcode_user_entered")
    return df
