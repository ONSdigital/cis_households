from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def match_type_blood(df: DataFrame):
    """Populate match type columns to illustrate how the
    blood data maps to the survey data."""
    df = df.withColumn("match_type_blood",
        F.when(
            F.col("file_date") - F.col("blood_sample_received_date"),
            '"lab orphan" - send to QC for investigation?'
        ).when(
            (F.col("blood_sample_barcode").isNull())
            &(
                ((F.col("file_date") - F.col("survey_completed_datetime")) >= 7)|
                ((F.col("file_date") - F.col("participant_completion_window_end_datetime")) >= 7)
            ),
            "survey orphan"
        ).when(
            F.col("blood_sample_barcode").isNotNull(),
            "matched"
        )
    )