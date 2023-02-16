import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.merge import union_multiple_tables
from cishouseholds.pipeline.phm import match_type_blood
from cishouseholds.pipeline.phm import match_type_swab
from cishouseholds.pyspark_utils import get_or_create_spark_session


def lab_transformations(df: DataFrame, blood_lookup_df: DataFrame, swab_lookup_df: DataFrame) -> DataFrame:
    """Apply all transformations related to lab columns in order."""
    df = join_blood_lookup_df(df, blood_lookup_df).custom_checkpoint()
    df = join_swab_lookup_df(df, swab_lookup_df).custom_checkpoint()
    return df


def join_blood_lookup_df(df: DataFrame, blood_lookup_df: DataFrame) -> DataFrame:
    """Joins blood lookup to df and runs transformation logic post join"""
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
    """Joins swab lookup to df"""
    # Logical condition for filtering records that are unjoinable
    record_unjoinable = F.col("swab_sample_barcode").isNull()

    # Set columns required for analysis
    swab_lookup_df = swab_lookup_df.withColumn("swab_sample_barcode_survey_missing_lab", record_unjoinable)
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
