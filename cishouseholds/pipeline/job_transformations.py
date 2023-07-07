from typing import List
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_job_description_string
from cishouseholds.expressions import any_column_not_null
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.pipeline.lookup_and_regex_transformations import process_healthcare_regex
from cishouseholds.pyspark_utils import get_or_create_spark_session

# from cishouseholds.edit import update_work_main_job_changed


# this wall feed in data from the joined healthcare regex


def job_transformations(df: DataFrame):
    """apply all transformations in order related to a persons vocation."""
    df = fill_forwards(df).custom_checkpoint()
    return df


def preprocessing(df: DataFrame):
    """Apply transformations that must occur before all other transformations can be processed."""
    df = clean_job_description_string(df, "work_main_job_title")
    df = clean_job_description_string(df, "work_main_job_role")
    col_val_map = {
        "work_health_care_area": {
            "Secondary care for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services?": "Other",
            "Primary care - for example in a GP or dentist": "Primary",
            "Yes, in primary care, e.g. GP, dentist": "Primary",
            "Secondary care - for example in a hospital": "Secondary",
            "Another type of healthcare - for example mental health services": "Other",  # noqa: E501
        },
        "work_not_from_home_days_per_week": {"NA": "99", "N/A (not working/in education etc)": "99", "up to 1": "0.5"},
    }
    df = apply_value_map_multiple_columns(df, col_val_map)
    return df


def get_unprocessed_rows(df: DataFrame, lookup_df: Optional[DataFrame] = None):
    """"""
    join_on_columns = ["work_main_job_title", "work_main_job_role"]
    df = df.filter(any_column_not_null(join_on_columns))
    if lookup_df is None:
        df_to_process = df.dropDuplicates(join_on_columns)
    else:
        non_derived_rows = df.join(lookup_df, on=join_on_columns, how="leftanti")
        df_to_process = non_derived_rows.dropDuplicates(join_on_columns)
    partitions = int(get_or_create_spark_session().sparkContext.getConf().get("spark.sql.shuffle.partitions"))
    partitions = int(partitions / 2)
    df_to_process = df_to_process.repartition(partitions)
    print(
        f"     - creating regex lookup table from {df_to_process.count()} rows. This may take some time ... "
    )  # functional
    return df_to_process


def create_job_lookup(df: DataFrame, soc_lookup_df: DataFrame, lookup_df: Optional[DataFrame] = None):
    df_to_process = get_unprocessed_rows(df, lookup_df)
    df_to_process = process_healthcare_regex(df_to_process)
    df_to_process = df_to_process.join(soc_lookup_df, on=["work_main_job_title", "work_main_job_role"], how="left")

    processed_df = df_to_process.select(
        "work_main_job_title",
        "work_main_job_role",
        # from healthcare
        "work_direct_contact_patients_or_clients",
        "work_social_care_area",
        "work_health_care_area",
        "work_health_care_patient_facing",
        "work_patient_facing_clean",
        "work_social_care",
        "works_health_care",
        "work_nursing_or_residential_care_home",
        "regex_derived_job_sector",
        # soc codes
        "standard_occupational_classification_code",
    )
    if lookup_df is not None:
        return lookup_df.unionByName(processed_df)
    return processed_df


def repopulate_missing_from_original(df: DataFrame, columns_to_update: List[str]):
    """Attempt to update columns with their original values if they have been nullified"""
    for col in columns_to_update:
        if f"{col}_original" in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.col(f"{col}_original")))
        elif f"{col}_raw" in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.col(f"{col}_raw")))
    return df


def fill_forwards(df: DataFrame):
    """Takes most recent not null response and fills forward into nulls"""
    df = fill_forward_only_to_nulls(
        df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "work_main_job_title",
            "work_main_job_role",
            "work_sector",
            "work_sector_other",
            "work_health_care_area",
            "work_nursing_or_residential_care_home",
        ],
    )
    # tidy up health and social care specific variables
    df = df.withColumn(
        "work_health_care_area",
        F.when((~F.col("work_sector").rlike("^Health")), None).otherwise(F.col("work_health_care_area")),
    )

    df = df.withColumn(
        "work_nursing_or_residential_care_home",
        F.when(~((F.col("work_sector").rlike("^Health")) | (F.col("work_sector").rlike("^Social"))), None).otherwise(
            F.col("work_nursing_or_residential_care_home")
        ),
    )

    return df


def data_dependent_derivations(df: DataFrame) -> DataFrame:
    """Apply transformations that require all data to have been previously filled over rows."""
    df = df.withColumn("work_main_job_title_and_role", F.concat_ws(" ", "work_main_job_title", "work_main_job_role"))

    window = Window.partitionBy("participant_id")
    patient_facing_percentage = F.sum(
        F.when(F.col("work_direct_contact_patients_or_clients") == "Yes", 1).otherwise(0)
    ).over(window) / F.sum(F.lit(1)).over(window)

    df = df.withColumn(
        "patient_facing_over_20_percent", F.when(patient_facing_percentage >= 0.2, "Yes").otherwise("No")
    )
    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status_v0",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    soc_code_col = "standard_occupational_classification_code"
    df = df.withColumn(soc_code_col, F.when(F.col(soc_code_col).isNull(), "uncodeable").otherwise(F.col(soc_code_col)))
    return df
