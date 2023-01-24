import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def match_type_blood(df: DataFrame):
    """Populate match type columns to illustrate how the
    blood data maps to the survey data."""
    options = [
        {"a": '"lab orphan" - send to QC for investigation?', "b": "survey orphan", "c": "matched", "d": "matched"},
        {
            "a": """test void" **do we send anything, who would we send to if can't match to a participant?""",
            "b": "test failed",
            "c": ["Positive for antibodies", "Positive at higher level for antibodies", "Negative for antibodies"],
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
