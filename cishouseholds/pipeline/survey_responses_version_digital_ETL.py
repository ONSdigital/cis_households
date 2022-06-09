import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_raw_copies
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.lit(None).cast("timestamp"))  # Placeholder for 2199

    df = transform_survey_responses_generic(df)
    
    df = assign_raw_copies(df, dont_know_columns)
    dont_know_mapping_dict = {"Prefer not to say": None, "Don't Know": None, "I don't know the type": None}
    df = apply_value_map_multiple_columns(
        df,
        {k: dont_know_mapping_dict for k in dont_know_columns},
    )

    return df
