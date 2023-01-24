from typing import Any
from typing import Dict
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_named_buckets


def match_type_blood(df: DataFrame):
    """Populate match type columns to illustrate how the
    blood data maps to the survey data."""
    options: List[Dict[str, Any]] = [
        {"a": "lab orphan", "b": "survey orphan", "c": "matched", "d": "matched"},
        {
            "a": "test void",
            "b": "test failed",
            "c": "encode",
            "d": "test void",
        },
        {"a": None, "b": None, "c": None, "d": "mapped to string"},
    ]
    column_names = ["match_type_blood", "match_result_blood", "void_reason_blood"]
    for col, option_set in zip(column_names, options):
        df = df.withColumn(
            "match_type_blood",
            F.when(F.col("file_date") - F.col("blood_sample_received_date"), option_set["a"])
            .when(
                (F.col("blood_sample_barcode").isNull())
                & (
                    ((F.col("file_date") - F.col("survey_completed_datetime")) >= 7)
                    | ((F.col("file_date") - F.col("participant_completion_window_end_datetime")) >= 7)
                ),
                option_set["b"],
            )
            .when(
                (F.col("blood_sample_barcode").isNotNull())
                & ((F.col("blood_sample_received_date") - F.col("blood_taken_datetime")) <= 28),
                option_set["c"],
            )
            .when(
                (F.col("blood_sample_barcode").isNotNull())
                & ((F.col("blood_sample_received_date") - F.col("blood_taken_datetime")) > 28),
                option_set["d"],
            ),
        )

    df = df.withColumn("match_result_blood", F.when(F.col("Monoclona quantitation (Colourmetric)")))
    df = assign_named_buckets(
        df,
        "Monoclona quantitation (Colourmetric)",
        "mapped_mqc",
        {0: "Negative for antibodies", 43: "Positive for antibodies", 180: "Positive for antibodies at a higher level"},
    )
    df = df.withColumn(
        "match_result_blood",
        F.when(F.col("match_result_blood") == "encode", F.col("mapped_mqc")).otherwise(F.col("match_result_blood")),
    ).drop("mapped_mqc")
    return df
