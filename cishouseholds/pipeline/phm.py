from typing import Any
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_named_buckets


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
    map = {
        "match_result_flu": "pcr_result_classification_flu",
        "match_result_rsv": "pcr_result_classification_rsv",
        "match_result_c19": "pcr_result_classification_c19",
    }
    for col, lookup in map.items():
        df = df.withColumn(col, F.when(F.col(col) == "encode", F.col(lookup)).otherwise(F.col(col)))
    return df


def assign_match_type(df: DataFrame, test_type: str):
    """map the pattern of input data to columns denoting the state of the test perfomed."""
    match_type_options: Dict[str, Any] = {"a": "lab orphan", "b": "survey orphan", "c": "matched", "d": "matched"}
    match_options: Dict[str, Any] = {"a": "test void", "b": "test failed", "c": "encode", "d": "test_void"}
    void_options: Dict[str, Any] = {"a": None, "b": None, "c": None, "d": "mapped to string"}
    column_names = [
        "match_type_swab",
        "match_result_flu",
        "match_result_rsv",
        "match_result_c19",
        "void_reason_flu",
        "void_reason_rsv",
        "void_reason_c19",
    ]
    n = 28 if test_type == "blood" else 21
    for col in column_names:
        if "match_type" in col:
            option_set = match_type_options
        elif "match" in col:
            option_set = match_options
        else:
            option_set = void_options

        df = df.withColumn(
            col,
            F.when(F.datediff(F.col("file_date"), F.col(f"{test_type}_sample_received_date")) > n, option_set["a"])
            .when(
                (F.col(f"{test_type}_sample_barcode_combined").isNull())
                & (
                    (F.datediff(F.col("file_date"), F.col("survey_completed_datetime")) >= 7)
                    | (F.datediff(F.col("file_date"), F.col("participant_completion_window_end_datetime")) >= 7)
                ),
                option_set["b"],
            )
            .when(
                (F.col(f"{test_type}_sample_barcode_combined").isNotNull())
                & (F.datediff(F.col(f"{test_type}_sample_received_date"), F.col(f"{test_type}_taken_datetime")) <= n),
                option_set["c"],
            )
            .when(
                (F.col(f"{test_type}_sample_barcode_combined").isNotNull())
                & (F.datediff(F.col(f"{test_type}_sample_received_date"), F.col(f"{test_type}_taken_datetime")) > n),
                option_set["d"],
            ),
        )
    return df
