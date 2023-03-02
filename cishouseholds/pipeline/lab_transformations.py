from typing import Any
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_named_buckets
from cishouseholds.merge import union_multiple_tables
from cishouseholds.pyspark_utils import get_or_create_spark_session


def lab_transformations(df: DataFrame, blood_lookup_df: DataFrame, swab_lookup_df: DataFrame) -> DataFrame:
    """Apply all transformations related to lab columns in order."""
    df = join_blood_lookup_df(df, blood_lookup_df).custom_checkpoint()
    df = join_swab_lookup_df(df, swab_lookup_df).custom_checkpoint()
    return df


def join_blood_lookup_df(df: DataFrame, blood_lookup_df: DataFrame) -> DataFrame:
    """
    Joins blood lookup to df and runs transformation logic post join

    New columns:
    - blood_sample_barcode_survey_missing_lab
    - blood_sample_barcode_lab_missing_survey
    - match_type_blood
    - match_result_blood
    - void_reason_blood
    & all fields from blood_lookup_df

    Reference columns:
    - blood_sample_barcode
    - blood_taken
    - blood_taken_datetime
    - household_completion_window_status
    - survey_completed_datetime
    - survey_completion_status
    - participant_completion_window_end_datetime
    - file_date
    """
    # Logical condition for filtering records that are unjoinable
    record_unjoinable = F.col("blood_sample_barcode").isNull()

    # Set columns required for analysis
    blood_lookup_df = blood_lookup_df.withColumn("blood_sample_barcode_survey_missing_lab", record_unjoinable)
    df = df.withColumn("blood_sample_barcode_lab_missing_survey", record_unjoinable)

    # Create unjoinable df to streamline processing time
    unjoinable_df = df.filter(record_unjoinable)

    # Run logic on df containing only joinable records
    df = df.filter(~record_unjoinable)
    df = df.join(blood_lookup_df, on="blood_sample_barcode", how="fullouter")

    # Join unjoinable records back to df and repartition
    df = union_multiple_tables([df, unjoinable_df])
    partitions = int(get_or_create_spark_session().sparkContext.getConf().get("spark.sql.shuffle.partitions"))
    partitions = int(partitions / 2)
    df = df.repartition(partitions)

    # Run required transformations
    df = match_type_blood(df)
    joinable = (
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
        & (F.col("participant_completion_window_start_datetime") <= F.col("blood_sample_arrayed_date"))
        & (F.col("match_type_blood").isNull())
    )
    for col in blood_lookup_df.columns:  # set cols to none if not joinable
        df = df.withColumn(col, F.when(joinable, F.col(col)).otherwise(None))
    return df


def join_swab_lookup_df(df: DataFrame, swab_lookup_df: DataFrame) -> DataFrame:
    """
    Joins swab lookup to df

    New columns:
    - swab_sample_barcode_survey_missing_lab
    - swab_sample_barcode_lab_missing_survey
    - match_type_c19
    - match_result_c19
    - void_reason_c19
    - match_type_rsv
    - match_result_rsv
    - void_reason_rsv
    - match_type_flu
    - match_result_flu
    - void_reason_flu
    & all fields from swab_lookup_df

    Reference columns:
    - blood_sample_barcode

    - blood_taken
    - blood_taken_datetime
    - household_completion_window_status
    - survey_completed_datetime
    - survey_completion_status
    - participant_completion_window_end_datetime
    - file_date
    """
    # Logical condition for filtering records that are unjoinable
    record_unjoinable = F.col("swab_sample_barcode").isNull()

    # Set columns required for analysis
    swab_lookup_df = swab_lookup_df.withColumn("swab_sample_barcode_survey_missing_lab", record_unjoinable)
    # Current lookup doesn't have a received date on the labs file so use result record as proxy
    swab_lookup_df = swab_lookup_df.withColumn("swab_sample_received_date", F.col("pcr_result_recorded_datetime"))
    df = df.withColumn("swab_sample_barcode_lab_missing_survey", record_unjoinable)

    # Create unjoinable df to streamline processing time
    unjoinable_df = df.filter(record_unjoinable)

    # Run logic on df containing only joinable records
    df = df.filter(~record_unjoinable)
    df = df.join(swab_lookup_df, on="swab_sample_barcode", how="fullouter")

    # Join unjoinable records back to df and repartition
    df = union_multiple_tables([df, unjoinable_df])
    partitions = int(get_or_create_spark_session().sparkContext.getConf().get("spark.sql.shuffle.partitions"))
    partitions = int(partitions / 2)
    df = df.repartition(partitions)

    # Run required transformations
    df = match_type_swab(df)
    return df


def match_type_blood(df: DataFrame):
    """Populate match type columns to illustrate how the
    blood data maps to the survey data."""
    df = assign_match_type(df, "blood")
    df = assign_named_buckets(
        df,
        "antibody_test_result_value",
        "mapped_mqc",
        {0: "Negative for antibodies", 43: "Positive for antibodies", 180: "Positive for antibodies at a higher level"},
    )
    df = df.withColumn(
        "match_result_blood",
        F.when(F.col("match_result_blood") == "encode", F.col("mapped_mqc")).otherwise(F.col("match_result_blood")),
    ).drop("mapped_mqc")
    return df


def match_type_swab(df: DataFrame):
    """Populate match type columns to illustrate how the
    swab data maps to the survey data."""
    df = assign_match_type(df, "swab")
    # No encoded remapping required for current responses
    # map = {
    #     "match_result_flu": "pcr_result_classification_flu",
    #     "match_result_rsv": "pcr_result_classification_rsv",
    #     "match_result_c19": "pcr_result_classification_c19",
    # }
    # for col, lookup in map.items():
    #     df = df.withColumn(col, F.when(F.col(col) == "encode", F.col(lookup)).otherwise(F.col(col)))
    return df


def assign_match_type(df: DataFrame, test_type: str):
    """map the pattern of input data to columns denoting the state of the test perfomed."""
    match_type_options: Dict[str, Any] = {"a": "lab orphan", "b": "survey orphan", "c": "matched", "d": "matched"}
    match_options: Dict[str, Any] = {"a": "test void", "b": "test failed", "c": "encode", "d": "test void"}
    void_options: Dict[str, Any] = {"a": None, "b": None, "c": None, "d": "mapped to string"}
    if test_type == "blood":
        column_names = ["match_type_blood", "match_result_blood", "void_reason_blood"]
        n = 28
    else:
        column_names = [
            "match_type_swab",
            "match_result_flu",
            "match_result_rsv",
            "match_result_c19",
            "void_reason_flu",
            "void_reason_rsv",
            "void_reason_c19",
        ]
        n = 21

    for col in column_names:
        if "match_type" in col:
            option_set = match_type_options
        elif "match" in col:
            option_set = match_options
        else:
            option_set = void_options

        df = df.withColumn(
            col,
            F.when(
                # missing from survey
                F.col(f"{test_type}_sample_barcode_lab_missing_survey")
                & (F.datediff(F.col("file_date"), F.col(f"{test_type}_sample_received_date")) > n),
                option_set["a"],
            )
            .when(
                # missing from lab
                F.col(f"{test_type}_sample_barcode_survey_missing_lab")
                & (
                    (F.datediff(F.col("file_date"), F.col("survey_completed_datetime")) >= 7)
                    | (F.datediff(F.col("file_date"), F.col("participant_completion_window_end_datetime")) >= 7)
                ),
                option_set["b"],
            )
            .when(
                # present for both lab and survey
                (
                    (~F.col(f"{test_type}_sample_barcode_survey_missing_lab"))
                    & (~F.col(f"{test_type}_sample_barcode_lab_missing_survey"))
                )
                & (F.datediff(F.col(f"{test_type}_sample_received_date"), F.col(f"{test_type}_taken_datetime")) <= n),
                option_set["c"],
            )
            .when(
                # present for both lab and survey
                (
                    (~F.col(f"{test_type}_sample_barcode_survey_missing_lab"))
                    & (~F.col(f"{test_type}_sample_barcode_lab_missing_survey"))
                )
                & (F.datediff(F.col(f"{test_type}_sample_received_date"), F.col(f"{test_type}_taken_datetime")) > n),
                option_set["d"],
            ),
        )
    return df
