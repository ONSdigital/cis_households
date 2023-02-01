import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.derive import assign_work_patient_facing_now
from cishouseholds.derive import assign_work_person_facing_now
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import flag_records_for_childcare_v0_rules
from cishouseholds.derive import flag_records_for_childcare_v1_rules
from cishouseholds.derive import flag_records_for_childcare_v2_b_rules
from cishouseholds.derive import flag_records_for_college_v0_rules
from cishouseholds.derive import flag_records_for_college_v1_rules
from cishouseholds.derive import flag_records_for_college_v2_rules
from cishouseholds.derive import flag_records_for_furlough_rules_v0
from cishouseholds.derive import flag_records_for_furlough_rules_v1_a
from cishouseholds.derive import flag_records_for_furlough_rules_v1_b
from cishouseholds.derive import flag_records_for_furlough_rules_v2_a
from cishouseholds.derive import flag_records_for_furlough_rules_v2_b
from cishouseholds.derive import flag_records_for_not_working_rules_v0
from cishouseholds.derive import flag_records_for_not_working_rules_v1_a
from cishouseholds.derive import flag_records_for_not_working_rules_v1_b
from cishouseholds.derive import flag_records_for_not_working_rules_v2_a
from cishouseholds.derive import flag_records_for_not_working_rules_v2_b
from cishouseholds.derive import flag_records_for_retired_rules
from cishouseholds.derive import flag_records_for_school_v2_rules
from cishouseholds.derive import flag_records_for_self_employed_rules_v0
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v1_b
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_a
from cishouseholds.derive import flag_records_for_self_employed_rules_v2_b
from cishouseholds.derive import flag_records_for_uni_v0_rules
from cishouseholds.derive import flag_records_for_uni_v1_rules
from cishouseholds.derive import flag_records_for_uni_v2_rules
from cishouseholds.derive import flag_records_for_work_from_home_rules
from cishouseholds.derive import flag_records_for_work_location_null
from cishouseholds.derive import flag_records_for_work_location_student
from cishouseholds.derive import regex_match_result
from cishouseholds.edit import clean_job_description_string
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.impute import fill_backwards_work_status_v2
from cishouseholds.impute import fill_forward_from_last_change_marked_subset
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.regex.regex_patterns import at_school_pattern
from cishouseholds.regex.regex_patterns import at_university_pattern
from cishouseholds.regex.regex_patterns import childcare_pattern
from cishouseholds.regex.regex_patterns import furloughed_pattern
from cishouseholds.regex.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.regex.regex_patterns import not_working_pattern
from cishouseholds.regex.regex_patterns import retired_regex_pattern
from cishouseholds.regex.regex_patterns import self_employed_regex
from cishouseholds.regex.regex_patterns import work_from_home_pattern

# this wall feed in data from the joined healthcare regex


def job_transformations(df: DataFrame):
    """apply all transformations in order related to a persons vocation."""
    df = fill_forwards(df).custom_checkpoint()
    df = data_dependent_transformations(df).custom_checkpoint()
    return df


def preprocessing(df: DataFrame):
    """"""
    df = clean_job_description_string(df, "work_main_job_title")
    df = clean_job_description_string(df, "work_main_job_role")
    df = update_column_values_from_map(
        df,
        "work_not_from_home_days_per_week",
        {"NA": "99", "N/A (not working/in education etc)": "99", "up to 1": "0.5"},
    )
    return df


def fill_forwards(df: DataFrame):
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
    return df


def data_dependent_transformations(df: DataFrame) -> DataFrame:
    """Apply transformations that require all data to have been previously filled over rows."""
    df = df.withColumn("work_main_job_title_and_role", F.concat_ws(" ", "work_main_job_title", "work_main_job_role"))
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
    df = assign_work_status_group(df, "work_status_group", "work_status_v0")

    window = Window.partitionBy("participant_id")
    patient_facing_percentage = F.sum(
        F.when(F.col("work_direct_contact_patients_or_clients") == "Yes", 1).otherwise(0)
    ).over(window) / F.sum(F.lit(1)).over(window)

    df = df.withColumn(
        "patient_facing_over_20_percent", F.when(patient_facing_percentage >= 0.2, "Yes").otherwise("No")
    )
    return df


def reclassify_work_variables(df: DataFrame, drop_original_variables: bool = True) -> DataFrame:
    """
    Reclassify work-related variables based on rules & regex patterns

    Parameters
    ----------
    df
        The dataframe containing the work-status related variables we want to edit
    drop_original_variables
        Set this to False if you want to retain the original variables so you can compare
        before & after edits.
    """
    spark_session = get_or_create_spark_session()
    # Work from Home
    working_from_home_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=work_from_home_pattern.positive_regex_pattern,
        negative_regex_pattern=work_from_home_pattern.negative_regex_pattern,
    )
    # Rule_id: 1000
    update_work_location = flag_records_for_work_from_home_rules() & working_from_home_regex_hit

    # Furlough
    furlough_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=furloughed_pattern.positive_regex_pattern,
        negative_regex_pattern=furloughed_pattern.negative_regex_pattern,
    )

    # Rule_id: 2000
    update_work_status_furlough_v0 = furlough_regex_hit & flag_records_for_furlough_rules_v0()
    # Rule_id: 2001
    update_work_status_furlough_v1_a = furlough_regex_hit & flag_records_for_furlough_rules_v1_a()
    # Rule_id: 2002
    update_work_status_furlough_v1_b = furlough_regex_hit & flag_records_for_furlough_rules_v1_b()
    # Rule_id: 2003
    update_work_status_furlough_v2_a = furlough_regex_hit & flag_records_for_furlough_rules_v2_a()
    # Rule_id: 2004
    update_work_status_furlough_v2_b = furlough_regex_hit & flag_records_for_furlough_rules_v2_b()

    # Self-Employed
    self_employed_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=self_employed_regex.positive_regex_pattern,
        negative_regex_pattern=self_employed_regex.negative_regex_pattern,
    )

    # Rule_id: 3000
    update_work_status_self_employed_v0 = self_employed_regex_hit & flag_records_for_self_employed_rules_v0()
    # Rule_id: 3001
    update_work_status_self_employed_v1_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_a()
    # Rule_id: 3002
    update_work_status_self_employed_v1_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v1_b()
    # Rule_id: 3003
    update_work_status_self_employed_v2_a = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_a()
    # Rule_id: 3004
    update_work_status_self_employed_v2_b = self_employed_regex_hit & flag_records_for_self_employed_rules_v2_b()

    # Retired
    retired_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=retired_regex_pattern.positive_regex_pattern,
        negative_regex_pattern=retired_regex_pattern.negative_regex_pattern,
    )

    # Rule_id: 4000, 4001, 4002
    update_work_status_retired = retired_regex_hit | flag_records_for_retired_rules()

    # Not-working
    not_working_regex_hit = (
        regex_match_result(
            columns_to_check_in=["work_main_job_title", "work_main_job_role"],
            positive_regex_pattern=not_working_pattern.positive_regex_pattern,
            negative_regex_pattern=not_working_pattern.negative_regex_pattern,
        )
        & ~working_from_home_regex_hit  # type: ignore
    )

    # Rule_id: 5000
    update_work_status_not_working_v0 = not_working_regex_hit & flag_records_for_not_working_rules_v0()
    # Rule_id: 5001
    update_work_status_not_working_v1_a = not_working_regex_hit & flag_records_for_not_working_rules_v1_a()
    # Rule_id: 5002
    update_work_status_not_working_v1_b = not_working_regex_hit & flag_records_for_not_working_rules_v1_b()
    # Rule_id: 5003
    update_work_status_not_working_v2_a = not_working_regex_hit & flag_records_for_not_working_rules_v2_a()
    # Rule_id: 5004
    update_work_status_not_working_v2_b = not_working_regex_hit & flag_records_for_not_working_rules_v2_b()

    # School/Student
    school_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_school_pattern.positive_regex_pattern,
        negative_regex_pattern=at_school_pattern.negative_regex_pattern,
    )

    college_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=in_college_or_further_education_pattern.positive_regex_pattern,
        negative_regex_pattern=in_college_or_further_education_pattern.negative_regex_pattern,
    )

    university_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=at_university_pattern.positive_regex_pattern,
        negative_regex_pattern=at_university_pattern.negative_regex_pattern,
    )

    # Childcare
    childcare_regex_hit = regex_match_result(
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        positive_regex_pattern=childcare_pattern.positive_regex_pattern,
        negative_regex_pattern=childcare_pattern.negative_regex_pattern,
    )

    age_under_16 = F.col("age_at_visit") < F.lit(16)
    age_over_four = F.col("age_at_visit") > F.lit(4)

    # Rule_id: 6000
    update_work_status_student_v0 = (
        (school_regex_hit & flag_records_for_school_v2_rules())
        | (university_regex_hit & flag_records_for_uni_v0_rules())
        | (college_regex_hit & flag_records_for_college_v0_rules())
        | (age_over_four & age_under_16)
    )

    # Rule_id: 6001
    update_work_status_student_v0_a = (childcare_regex_hit & flag_records_for_childcare_v0_rules()) | (
        school_regex_hit & flag_records_for_childcare_v0_rules()
    )

    # Rule_id: 6002
    update_work_status_student_v1_a = (
        (school_regex_hit & flag_records_for_school_v2_rules())
        | (university_regex_hit & flag_records_for_uni_v1_rules())
        | (college_regex_hit & flag_records_for_college_v1_rules())
        | (age_over_four & age_under_16)
    )

    # Rule_id: 6003
    update_work_status_student_v1_c = (childcare_regex_hit & flag_records_for_childcare_v1_rules()) | (
        school_regex_hit & flag_records_for_childcare_v1_rules()
    )

    # Rule_id: 6004
    update_work_status_student_v2_e = (childcare_regex_hit & flag_records_for_childcare_v2_b_rules()) | (
        school_regex_hit & flag_records_for_childcare_v2_b_rules()
    )

    # Rule_id: 6005
    update_work_status_student_v2_a = (school_regex_hit & flag_records_for_school_v2_rules()) | (
        age_over_four & age_under_16
    )

    # Rule_id: 6006
    update_work_status_student_v2_b = college_regex_hit & flag_records_for_college_v2_rules()

    # Rule_id: 6007
    update_work_status_student_v2_c = university_regex_hit & flag_records_for_uni_v2_rules()

    # Rule_id: 6008
    update_work_location_general = flag_records_for_work_location_null() | flag_records_for_work_location_student()

    # Please note the order of *_edited columns, these must come before the in-place updates

    # first start by taking a copy of the original work variables
    _df = (
        df.withColumn("work_location_original", F.col("work_location"))
        .withColumn("work_status_v0_original", F.col("work_status_v0"))
        .withColumn("work_status_v1_original", F.col("work_status_v1"))
        .withColumn("work_status_v2_original", F.col("work_status_v2"))
        .withColumn(
            "work_location",
            F.when(update_work_location, F.lit("Working from home")).otherwise(F.col("work_location")),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_self_employed_v0, F.lit("Self-employed")).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_a, F.lit("Self-employed and currently working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_self_employed_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_self_employed_v2_a,
                F.lit("Self-employed and currently working"),
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_self_employed_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
    )

    _df2 = spark_session.createDataFrame(_df.rdd, schema=_df.schema)  # breaks lineage to avoid Java OOM Error

    _df3 = (
        _df2.withColumn(
            "work_status_v0",
            F.when(update_work_status_student_v0 | update_work_status_student_v0_a, F.lit("Student")).otherwise(
                F.col("work_status_v0")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_a, F.lit("5y and older in full-time education")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_student_v1_c, F.lit("Child under 5y attending child care")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_a, F.lit("4-5y and older at school/home-school")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_b, F.lit("Attending college or FE (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(
                update_work_status_student_v2_c, F.lit("Attending university (including if temporarily absent)")
            ).otherwise(F.col("work_status_v2")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_student_v2_e, F.lit("Child under 4-5y attending child care")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(
                update_work_status_retired, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v1")),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_retired, F.lit("Retired")).otherwise(F.col("work_status_v2")),
        )
    )

    _df4 = spark_session.createDataFrame(_df3.rdd, schema=_df3.schema)  # breaks lineage to avoid Java OOM Error

    _df5 = (
        _df4.withColumn(
            "work_status_v0",
            F.when(
                update_work_status_not_working_v0, F.lit("Not working (unemployed, retired, long-term sick etc.)")
            ).otherwise(F.col("work_status_v0")),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_not_working_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_not_working_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v0",
            F.when(update_work_status_furlough_v0, F.lit("Furloughed (temporarily not working)")).otherwise(
                F.col("work_status_v0")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v1",
            F.when(update_work_status_furlough_v1_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v1")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_a, F.lit("Employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_status_v2",
            F.when(update_work_status_furlough_v2_b, F.lit("Self-employed and currently not working")).otherwise(
                F.col("work_status_v2")
            ),
        )
        .withColumn(
            "work_location",
            F.when(
                update_work_location_general,
                F.lit("Not applicable, not currently working"),
            ).otherwise(F.col("work_location")),
        )
    )

    if drop_original_variables:
        # replace original versions with their cleaned versions
        _df5 = _df5.drop(
            "work_location_original",
            "work_status_v0_original",
            "work_status_v1_original",
            "work_status_v2_original",
        )

    return _df5


def assign_work_classifications(df: DataFrame):
    """"""
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
    df = reclassify_work_variables(df, drop_original_variables=False)
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
