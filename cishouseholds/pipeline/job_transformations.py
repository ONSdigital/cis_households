from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import assign_work_status_group
from cishouseholds.edit import apply_value_map_multiple_columns
from cishouseholds.edit import clean_job_description_string
from cishouseholds.expressions import any_column_not_null
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change_marked_subset
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.merge import left_join_keep_right
from cishouseholds.pipeline.lookup_and_regex_transformations import process_healthcare_regex
from cishouseholds.pipeline.lookup_and_regex_transformations import reclassify_work_variables
from cishouseholds.pyspark_utils import get_or_create_spark_session


# this wall feed in data from the joined healthcare regex


def job_transformations(df: DataFrame, soc_lookup_df: DataFrame, job_lookup_df: Optional[DataFrame] = None):
    """apply all transformations in order related to a persons vocation."""
    df = preprocessing(df)
    job_lookup_df = create_job_lookup(df, soc_lookup_df=soc_lookup_df, lookup_df=job_lookup_df).custom_checkpoint()
    df = left_join_keep_right(
        left_df=df, right_df=job_lookup_df, join_on_columns=["work_main_job_title", "work_main_job_role"]
    )
    df = fill_forwards_and_backwards(df).custom_checkpoint()
    df = data_dependent_derivations(df).custom_checkpoint()
    return df, job_lookup_df


def preprocessing(df: DataFrame):
    """"""
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
    df_to_process = reclassify_work_variables(df_to_process)
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
        # from reclassify
        "work_status_v0",
        "work_status_v1",
        "work_status_v2",
        "work_location",
        # soc codes
        "standard_occupational_classification_code",
    )
    if lookup_df is not None:
        return lookup_df.unionByName(processed_df)
    return processed_df


def fill_forwards_and_backwards(df: DataFrame):
    df = fill_forward_from_last_change_marked_subset(
        df=df,
        fill_forward_columns=[
            "work_main_job_title",
            "work_main_job_role",
            "work_sector",
            "work_sector_other",
            "work_social_care",
            "work_health_care_patient_facing",
            "work_health_care_area",
            "work_nursing_or_residential_care_home",
            "work_direct_contact_patients_or_clients",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="work_main_job_changed",
        record_changed_value="Yes",
        dateset_version_column="survey_response_dataset_major_version",
        minimum_dateset_version=2,
    )
    df = fill_backwards_work_status_v2(
        df=df,
        date="visit_datetime",
        id="participant_id",
        fill_backward_column="work_status_v2",
        condition_column="work_status_v1",
        date_range=["2020-09-01", "2021-08-31"],
        condition_column_values=["5y and older in full-time education"],
        fill_only_backward_column_values=[
            "4-5y and older at school/home-school",
            "Attending college or FE (including if temporarily absent)",
            "Attending university (including if temporarily absent)",
        ],
    )
    df = fill_forward_only_to_nulls(
        df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "work_status_v0",
            "work_status_v1",
            "work_status_v2",
            "work_location",
            "work_not_from_home_days_per_week",
        ],
    )
    return df


def data_dependent_derivations(df: DataFrame) -> DataFrame:
    """Apply transformations that require all data to have been previously filled over rows."""
    df = df.withColumn("work_main_job_title_and_role", F.concat_ws(" ", "work_main_job_title", "work_main_job_role"))

    df = assign_work_status_group(df, "work_status_group", "work_status_v0")

    window = Window.partitionBy("participant_id")
    patient_facing_percentage = F.sum(
        F.when(F.col("work_direct_contact_patients_or_clients") == "Yes", 1).otherwise(0)
    ).over(window) / F.sum(F.lit(1)).over(window)

    df = df.withColumn(
        "patient_facing_over_20_percent", F.when(patient_facing_percentage >= 0.2, "Yes").otherwise("No")
    )
    df = assign_work_patient_facing_now(
        df,
        column_name_to_assign="work_patient_facing_now",
        age_column="age_at_visit",
        work_healthcare_column="work_health_care_patient_facing",
    )
    df = assign_work_person_facing_now(
        df,
        column_name_to_assign="work_person_facing_now",
        work_patient_facing_now_column="work_patient_facing_now",
        work_social_care_column="work_social_care",
        age_at_visit_column="age_at_visit",
    )
    # df = update_work_facing_now_column(
    #     df,
    #     "work_patient_facing_now",
    #     "work_status_v0",
    #     ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)", "Student"],
    # )
    return df
