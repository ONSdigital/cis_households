import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.phm import match_type_blood
from cishouseholds.pipeline.phm import match_type_swab


def lab_transformations(df: DataFrame, blood_lookup_df: DataFrame, swab_lookup_df: DataFrame) -> DataFrame:
    """Apply all transformations related to lab columns in order."""
    df = join_blood_lookup_df(df, blood_lookup_df).custom_checkpoint()
    df = join_swab_lookup_df(df, blood_lookup_df).custom_checkpoint()
    return df


def join_blood_lookup_df(df: DataFrame, blood_lookup_df: DataFrame) -> DataFrame:
    """"""
    blood_lookup_df = blood_lookup_df.withColumn(
        "blood_sample_barcode_survey_missing_lab", F.col("blood_sample_barcode").isNull()
    )
    df = df.withColumn("blood_sample_barcode_lab_missing_survey", F.col("blood_sample_barcode").isNull())
    df = df.join(df, blood_lookup_df, how="fullouter", on=["blood_sample_barcode"])
    joinable = F.when(
        (
            (F.col("blood_taken").isNotNull())
            & (
                (F.col("household_completion_window_status") == "Closed")
                | (
                    (F.col("survey_completed_datetime").isNotNull())
                    | (F.col("survey_completion_status") == "Submitted")
                )
            )
        )
        & (F.col("blood_taken_datetime") < F.col("blood_sample_arrayed_date"))
        & ((F.col("participant_completion_window_start_datetime")) <= (F.col("") == F.col("blood_sample_arrayed_date")))
        & (F.col("match_type_blood").isNull())
    )
    df = match_type_blood(df)
    for col in blood_lookup_df.columns:  # set cols to none if not joinable
        df = df.withColumn(F.when(joinable, F.col(col)))
    return df


def join_swab_lookup_df(df: DataFrame, blood_lookup_df: DataFrame) -> DataFrame:
    """"""
    blood_lookup_df = blood_lookup_df.withColumn(
        "swab_sample_barcode_survey_missing_lab", F.col("swab_sample_barcode").isNull()
    )
    df = df.join(df, blood_lookup_df, how="fullouter", on=["swab_sample_barcode"])
    df = df.withColumn("swab_sample_barcode_lab_missing_survey", F.col("swab_sample_barcode").isNull())
    df = match_type_swab(df)
    return df
