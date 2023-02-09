# flake8: noqa
from typing import List
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_regex_from_map
from cishouseholds.derive import assign_regex_match_result
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
from cishouseholds.edit import rename_column_names
from cishouseholds.expressions import any_column_not_null
from cishouseholds.expressions import array_contains_any
from cishouseholds.merge import null_safe_join
from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.phm import match_type_blood
from cishouseholds.pipeline.phm import match_type_swab
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.regex.healthcare_regex import healthcare_classification
from cishouseholds.regex.healthcare_regex import patient_facing_classification
from cishouseholds.regex.healthcare_regex import patient_facing_pattern
from cishouseholds.regex.healthcare_regex import priority_map
from cishouseholds.regex.healthcare_regex import roles_map
from cishouseholds.regex.healthcare_regex import social_care_classification
from cishouseholds.regex.regex_patterns import at_school_pattern
from cishouseholds.regex.regex_patterns import at_university_pattern
from cishouseholds.regex.regex_patterns import childcare_pattern
from cishouseholds.regex.regex_patterns import furloughed_pattern
from cishouseholds.regex.regex_patterns import in_college_or_further_education_pattern
from cishouseholds.regex.regex_patterns import not_working_pattern
from cishouseholds.regex.regex_patterns import retired_regex_pattern
from cishouseholds.regex.regex_patterns import self_employed_regex
from cishouseholds.regex.regex_patterns import work_from_home_pattern
from cishouseholds.regex.vaccine_regex import vaccine_regex_map
from cishouseholds.regex.vaccine_regex import vaccine_regex_priority_map


def transform_cis_soc_data(
    soc_lookup_df: DataFrame, inconsistences_resolution_df: DataFrame, join_on_columns: List[str]
) -> DataFrame:
    """
    transform and process cis soc data
    """

    drop_null_title_df = soc_lookup_df.filter(F.col("work_main_job_title").isNull()).withColumn(
        "drop_reason", F.lit("null job title")
    )

    # cleanup soc lookup df and resolve inconsistences
    soc_lookup_df = soc_lookup_df.filter(F.col("work_main_job_title").isNotNull())

    # allow nullsafe join on title as soc is sometimes assigned without job role
    soc_lookup_df = null_safe_join(
        soc_lookup_df, inconsistences_resolution_df, null_safe_on=join_on_columns, how="left"
    )

    soc_lookup_df = soc_lookup_df.drop_duplicates(["standard_occupational_classification_code", *join_on_columns])

    soc_lookup_df = soc_lookup_df.withColumn(
        "standard_occupational_classification_code",
        F.coalesce(F.col("resolved_soc_code"), F.col("standard_occupational_classification_code")),
    ).drop("resolved_soc_code")

    # normalise uncodeable values
    soc_lookup_df = soc_lookup_df.withColumn(
        "standard_occupational_classification_code",
        F.when(
            (F.col("standard_occupational_classification_code").rlike(r".*[^0-9].*|^\s*$"))
            | (F.col("standard_occupational_classification_code").isNull()),
            "uncodeable",
        ).otherwise(F.col("standard_occupational_classification_code")),
    )

    # decide on rows to drop
    soc_lookup_df = soc_lookup_df.withColumn(
        "LENGTH",
        F.length(
            F.when(
                F.col("standard_occupational_classification_code") != "uncodeable",
                F.col("standard_occupational_classification_code"),
            )
        ),
    ).orderBy(F.desc("LENGTH"))

    # create windows with descending soc code
    window = Window.partitionBy(*join_on_columns)

    # flag non specific soc codes and uncodeable codes
    soc_lookup_df = soc_lookup_df.withColumn(
        "drop_reason",
        F.when(
            (F.col("LENGTH") != F.max("LENGTH").over(window))
            | (F.col("standard_occupational_classification_code") == "uncodeable"),
            "more specific code available",
        ).otherwise(None),
    )
    retain_count = F.sum(F.when(F.col("drop_reason").isNull(), 1).otherwise(0)).over(window)
    # flag ambiguous codes from remaining set
    soc_lookup_df = soc_lookup_df.withColumn(
        "drop_reason",
        F.when(
            (retain_count > 1) & (F.col("drop_reason").isNull()),
            "ambiguous code",
        ).otherwise(F.col("drop_reason")),
    ).drop("LENGTH")

    # remove flag from first row of dropped set if all codes from group are flagged
    soc_lookup_df = soc_lookup_df.withColumn(
        "drop_reason", F.when(F.count("*").over(window) == 1, None).otherwise(F.col("drop_reason"))
    )
    resolved_df = soc_lookup_df.filter(F.col("drop_reason").isNull()).drop("drop_reason", "ROW_NUMBER")
    duplicate_df = soc_lookup_df.filter(F.col("drop_reason").isNotNull()).drop("ROW_NUMBER")

    return duplicate_df.unionByName(drop_null_title_df), resolved_df


def transform_from_lookups(
    df: DataFrame, cohort_lookup: DataFrame, travel_countries_lookup: DataFrame, tenure_group: DataFrame
):
    cohort_lookup = cohort_lookup.withColumnRenamed("participant_id", "cohort_participant_id")
    df = df.join(
        F.broadcast(cohort_lookup),
        how="left",
        on=((df.participant_id == cohort_lookup.cohort_participant_id) & (df.study_cohort == cohort_lookup.old_cohort)),
    ).drop("cohort_participant_id")
    df = df.withColumn("study_cohort", F.coalesce(F.col("new_cohort"), F.col("study_cohort"))).drop(
        "new_cohort", "old_cohort"
    )
    df = df.join(
        F.broadcast(travel_countries_lookup.withColumn("REPLACE_COUNTRY", F.lit(True))),
        how="left",
        on=df.been_outside_uk_last_country == travel_countries_lookup.been_outside_uk_last_country_old,
    )
    df = df.withColumn(
        "been_outside_uk_last_country",
        F.when(F.col("REPLACE_COUNTRY"), F.col("been_outside_uk_last_country_new")).otherwise(
            F.col("been_outside_uk_last_country"),
        ),
    ).drop("been_outside_uk_last_country_old", "been_outside_uk_last_country_new", "REPLACE_COUNTRY")

    for key, value in column_name_maps["tenure_group_variable_map"].items():
        tenure_group = tenure_group.withColumnRenamed(key, value)

    df = df.join(tenure_group, on=(df["ons_household_id"] == tenure_group["UAC"]), how="left").drop("UAC")
    return df


def nims_transformations(df: DataFrame, **kwargs: dict) -> DataFrame:
    """Clean and transform NIMS data after reading from table."""
    df = rename_column_names(df, column_name_maps["nims_column_name_map"])
    df = assign_column_to_date_string(df, "nims_vaccine_dose_1_date", reference_column="nims_vaccine_dose_1_datetime")
    df = assign_column_to_date_string(df, "nims_vaccine_dose_2_date", reference_column="nims_vaccine_dose_2_datetime")

    # TODO: Derive nims_linkage_status, nims_vaccine_classification, nims_vaccine_dose_1_time, nims_vaccine_dose_2_time
    return df


def blood_past_positive_transformations(df: DataFrame, **kwargs: dict) -> DataFrame:
    """Run required post-join transformations for blood_past_positive"""
    df = df.withColumn("blood_past_positive_flag", F.when(F.col("blood_past_positive").isNull(), 0).otherwise(1))
    return df


def design_weights_lookup_transformations(df: DataFrame, **kwargs: dict) -> DataFrame:
    """Selects only required fields from the design_weight_lookup"""
    design_weight_columns = ["scaled_design_weight_swab_non_adjusted", "scaled_design_weight_antibodies_non_adjusted"]
    df = df.select(*design_weight_columns, "ons_household_id")
    return df


def derive_overall_vaccination(df: DataFrame, **kwargs: dict) -> DataFrame:
    """Derive overall vaccination status from NIMS and CIS data."""
    return df


def ordered_household_id_tranformations(df: DataFrame, **kwargs: dict) -> DataFrame:
    """Read in a survey responses table and join it onto the participants extract to ensure matching ordered household ids"""
    join_on_columns = ["ons_household_id", "ordered_household_id"]
    df = df.select(join_on_columns).distinct()
    return df


def process_vaccine_regex(df: DataFrame, vaccine_type_col: str) -> DataFrame:
    """Add result of vaccine regex pattern matchings"""

    df = df.select(vaccine_type_col)

    df = assign_regex_from_map(
        df=df,
        column_name_to_assign="cis_covid_vaccine_type_corrected",
        reference_columns=[vaccine_type_col],
        map=vaccine_regex_map,
        priority_map=vaccine_regex_priority_map,
    )
    df = df.withColumn(
        vaccine_type_col, F.when(F.col(vaccine_type_col).isNull(), "Don't know type").otherwise(F.col(vaccine_type_col))
    )
    df = df.withColumnRenamed(vaccine_type_col, "cis_covid_vaccine_type_other_raw")
    # df = df.filter(F.col("cis_covid_vaccine_type_corrected").isNotNull())
    return df


def process_healthcare_regex(df: DataFrame) -> DataFrame:
    """Add result of various healthcare regex pattern matchings"""
    # df = df.drop(
    #     "work_health_care_patient_facing_original",
    #     "work_social_care_original",
    #     "work_care_nursing_home_original",
    #     "work_direct_contact_patients_or_clients_original",
    # )

    df = df.withColumn("work_main_job_title", F.upper(F.col("work_main_job_title")))
    df = df.withColumn("work_main_job_role", F.upper(F.col("work_main_job_role")))

    df = assign_regex_from_map(
        df=df,
        column_name_to_assign="regex_derived_job_sector",
        reference_columns=["work_main_job_title", "work_main_job_role"],
        map=roles_map,
        priority_map=priority_map,
    )
    # create healthcare area flag
    df = df.withColumn("work_health_care_area", F.lit(None))
    for healthcare_type, roles in healthcare_classification.items():  # type: ignore
        df = df.withColumn(
            "work_health_care_area",
            F.when(array_contains_any("regex_derived_job_sector", roles), healthcare_type).otherwise(
                F.col("work_health_care_area")
            ),  # type: ignore
        )
    # TODO: need to exclude healthcare types from social care matching
    df = df.withColumn("work_social_care_area", F.lit(None))
    for social_care_type, roles in social_care_classification.items():  # type: ignore
        df = df.withColumn(
            "work_social_care_area",
            F.when(F.col("work_health_care_area").isNotNull(), None)
            .when(array_contains_any("regex_derived_job_sector", roles), social_care_type)
            .otherwise(F.col("work_social_care_area")),  # type: ignore
        )

    df = df.withColumn(
        "work_nursing_or_residential_care_home",
        F.when(
            array_contains_any("regex_derived_job_sector", ["residential_care"]),
            "Yes",
        ).otherwise("No"),
    )

    # add boolean flags for working in healthcare or socialcare

    df = df.withColumn("works_health_care", F.when(F.col("work_health_care_area").isNotNull(), "Yes").otherwise("No"))

    df = assign_regex_match_result(
        df=df,
        columns_to_check_in=["work_main_job_title", "work_main_job_role"],
        column_name_to_assign="work_direct_contact_patients_or_clients_regex_derived",
        positive_regex_pattern=patient_facing_pattern.positive_regex_pattern,
        negative_regex_pattern=patient_facing_pattern.negative_regex_pattern,
    )
    df = df.withColumn(
        "work_direct_contact_patients_or_clients",
        F.when(
            (F.col("work_health_care_area_original") == F.col("work_health_care_area"))
            & (F.col("work_direct_contact_patients_or_clients").isNotNull()),
            F.col("work_direct_contact_patients_or_clients"),
        )
        .when(
            (
                (F.col("works_health_care") == "Yes")
                | (F.col("work_direct_contact_patients_or_clients_regex_derived") == True)
            )
            & (~array_contains_any("regex_derived_job_sector", patient_facing_classification["N"])),
            "Yes",
        )
        .otherwise("No"),
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_health_care_patient_facing",
        [
            ["No", ["No", None]],
            ["No", ["Yes", None]],
            ["Yes, primary care, patient-facing", ["Yes", "Primary"]],
            ["Yes, secondary care, patient-facing", ["Yes", "Secondary"]],
            ["Yes, other healthcare, patient-facing", ["Yes", "Other"]],
            ["Yes, primary care, non-patient-facing", ["No", "Primary"]],
            ["Yes, secondary care, non-patient-facing", ["No", "Secondary"]],
            ["Yes, other healthcare, non-patient-facing", ["No", "Other"]],
        ],
        ["work_direct_contact_patients_or_clients", "work_health_care_area"],
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_social_care",
        [
            ["No", ["No", None]],
            ["No", ["Yes", None]],
            ["Yes, care/residential home, resident-facing", ["Yes", "Care/Residential home"]],
            ["Yes, other social care, resident-facing", ["Yes", "Other"]],
            ["Yes, care/residential home, non-resident-facing", ["No", "Care/Residential home"]],
            ["Yes, other social care, non-resident-facing", ["No", "Other"]],
        ],
        ["work_direct_contact_patients_or_clients", "work_social_care_area"],
    )
    df = assign_column_value_from_multiple_column_map(
        df,
        "work_patient_facing_clean",
        [["Yes", ["Yes", "Yes"]], ["No", ["No", "Yes"]], ["Not working in health care", ["No", "No"]]],
        ["work_direct_contact_patients_or_clients", "works_health_care"],
    )
    # work_status_columns = [col for col in df.columns if "work_status_" in col]
    # for work_status_column in work_status_columns:
    #     df = df.withColumn(
    #         work_status_column,
    #         F.when(F.col("not_working"), "not working")
    #         .when(F.col("at_school") | F.col("at_university"), "student")
    #         .when(F.array_contains(F.col("regex_derived_job_sector"), "apprentice"), "working")
    #         .otherwise(F.col(work_status_column)),
    #     )
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
