import pyspark.sql.functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import assign_raw_copies
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.pipeline.survey_responses_version_2_ETL import transform_survey_responses_generic


def digital_specific_transformations(df):
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 3)
    df = df.withColumn("face_covering_outside_of_home", F.lit(None).cast("string"))
    df = df.withColumn("visit_id", F.col("participant_completion_window_id"))
    df = df.withColumn("visit_datetime", F.col("digital_enrolment_invite_datetime"))
    df = transform_survey_responses_generic(df)
    df = assign_raw_copies(
        df,
        [
            "work_in_additional_paid_employment",
            "self_isolating",
            "illness_lasting_over_12_months",
            "ever_smoked_regularly",
            "currently_smokes_or_vapes",
            "hours_a_day_with_someone_else_at_home",
            "face_covering_work_or_education",
        ],
    )
    mapping_dict = {
        "Prefer not to say": None,
        "Don't Know": None,
    }
    df = apply_value_map_multiple_columns(
        df,
        {
            "work_in_additional_paid_employment": mapping_dict,
            "self_isolating": mapping_dict,
            "illness_lasting_over_12_months": mapping_dict,
            "ever_smoked_regularly": mapping_dict,
            "currently_smokes_or_vapes": mapping_dict,
            "hours_a_day_with_someone_else_at_home": mapping_dict,
            "face_covering_work_or_education": mapping_dict,
        },
    )
    return df
