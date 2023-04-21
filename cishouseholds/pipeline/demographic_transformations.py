from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.derive import assign_age_group_school_year
from cishouseholds.derive import assign_column_from_mapped_list_key
from cishouseholds.derive import assign_column_regex_match
from cishouseholds.derive import assign_ethnicity_white
from cishouseholds.derive import assign_household_participant_count
from cishouseholds.derive import assign_multigenerational
from cishouseholds.derive import assign_outward_postcode
from cishouseholds.derive import assign_work_status_group
from cishouseholds.derive import clean_postcode
from cishouseholds.derive import derive_age_based_columns
from cishouseholds.edit import update_column_values_from_map
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.impute import fill_backwards_overriding_not_nulls
from cishouseholds.impute import fill_forward_from_last_change
from cishouseholds.impute import fill_forward_only_to_nulls
from cishouseholds.impute import impute_and_flag
from cishouseholds.impute import impute_by_distribution
from cishouseholds.impute import impute_by_k_nearest_neighbours
from cishouseholds.impute import impute_by_mode
from cishouseholds.impute import impute_date_by_k_nearest_neighbours
from cishouseholds.impute import merge_previous_imputed_values
from cishouseholds.merge import left_join_keep_right
from cishouseholds.pipeline.config import get_config


def demographic_transformations(
    df: DataFrame,
    geography_lookup_df: DataFrame,
    rural_urban_lookup_df: DataFrame,
    imputed_value_lookup_df: Optional[DataFrame] = None,
):
    """
    Modify the unioned survey response files by transforming the demographic data columns.

    call all functions in order necessary to update the demographic columns.
    """
    log_directory: str = get_config()["imputation_log_directory"]

    df = generic_processing(df).custom_checkpoint()
    df = join_statistical_geographies(
        df=df, geography_lookup_df=geography_lookup_df, rural_urban_lookup_df=rural_urban_lookup_df
    ).custom_checkpoint()
    df = fill_forwards_and_backwards(df).custom_checkpoint()
    df = ethnicity_transformations(df).custom_checkpoint()
    df = derive_people_in_household_count(df).custom_checkpoint()
    imputed_demographic_columns_df = impute_key_columns(df, imputed_value_lookup_df, log_directory).custom_checkpoint()
    df = geography_dependent_transformations(
        df=df, imputed_demographic_columns_df=imputed_demographic_columns_df
    ).custom_checkpoint()
    return df, imputed_value_lookup_df


def generic_processing(df: DataFrame):
    """
    Edit:
    - study_cohort
    - postcode
    - been_outside_uk

    Derived columns:
    - work_status_group
    - bad_email
    - consent_summary
    """
    # EDIT
    df = df.withColumn(
        "study_cohort", F.when(F.col("study_cohort").isNull(), "Original").otherwise(F.col("study_cohort"))
    )
    df = update_to_value_if_any_not_null(
        df=df,
        column_name_to_update="been_outside_uk",
        true_false_values=["Yes", "No"],
        column_list=["been_outside_uk_last_country", "been_outside_uk_last_return_date"],
    )
    df = clean_postcode(df, "postcode")
    # DERIVE
    df = assign_work_status_group(df, "work_status_group", "work_status_v0")
    df = assign_column_regex_match(
        df,
        "bad_email",
        reference_column="email_address",
        pattern=r"/^w+[+.w-]*@([w-]+.)*w+[w-]*.([a-z]{2,4}|d+)$/i",
    )
    # consent_cols = ["consent_16_visits", "consent_5_visits", "consent_1_visit"]

    # if all(col in df.columns for col in consent_cols):
    #     df = assign_consent_code(df, "consent_summary", reference_columns=consent_cols)
    return df


def join_statistical_geographies(
    df: DataFrame,
    geography_lookup_df: DataFrame,  # should include rural urban lookup,
    rural_urban_lookup_df: DataFrame,
) -> DataFrame:
    """Run required post-join transformations for replace_design_weights"""

    df = left_join_keep_right(df, geography_lookup_df, ["ons_household_id"])
    df = left_join_keep_right(df, rural_urban_lookup_df, ["cis_area_code_20"])

    df = df.withColumn(
        "local_authority_unity_authority_code",
        F.when(F.col("local_authority_unity_authority_code") == "E06000062", "E07000154")
        .when(F.col("local_authority_unity_authority_code") == "E06000061", "E07000156")
        .otherwise(F.col("local_authority_unity_authority_code")),
    )
    df = df.withColumn(
        "region_code",
        F.when(F.col("region_code") == "W92000004", "W99999999")
        .when(F.col("region_code") == "S92000003", "S99999999")
        .when(F.col("region_code") == "N92000002", "N99999999")
        .otherwise(F.col("region_code")),
    )
    return df


def ethnicity_transformations(df: DataFrame):
    """
    Derived:
    - ethnicity_group
    - ethnicity_white

    Edited:
    - ethnicity
    """
    ethnic_group_map = {
        "White": ["White-British", "White-Irish", "White-Gypsy or Irish Traveller", "Any other white background"],
        "Asian": [
            "Asian or Asian British-Indian",
            "Asian or Asian British-Pakistani",
            "Asian or Asian British-Bangladeshi",
            "Asian or Asian British-Chinese",
            "Any other Asian background",
        ],
        "Black": ["Black,Caribbean,African-African", "Black,Caribbean,Afro-Caribbean", "Any other Black background"],
        "Mixed": [
            "Mixed-White & Black Caribbean",
            "Mixed-White & Black African",
            "Mixed-White & Asian",
            "Any other Mixed background",
        ],
        "Other": ["Other ethnic group-Arab", "Any other ethnic group"],
    }
    ethnicity_value_map = {
        "African": "Black,Caribbean,African-African",
        "Caribbean": "Black,Caribbean,Afro-Caribbean",
        "Any other Black or African or Caribbean background": "Any other Black background",
        "Any other Black| African| Carribbean": "Any other Black background",
        "Any other Mixed/Multiple background": "Any other Mixed background",
        "Bangladeshi": "Asian or Asian British-Bangladeshi",
        "Chinese": "Asian or Asian British-Chinese",
        "English, Welsh, Scottish, Northern Irish or British": "White-British",
        "English| Welsh| Scottish| Northern Irish or British": "White-British",
        "Indian": "Asian or Asian British-Indian",
        "Irish": "White-Irish",
        "Pakistani": "Asian or Asian British-Pakistani",
        "White and Asian": "Mixed-White & Asian",
        "White and Black African": "Mixed-White & Black African",
        "White and Black Caribbean": "Mixed-White & Black Caribbean",
        "Roma": "White-Gypsy or Irish Traveller",
        "White-Roma": "White-Gypsy or Irish Traveller",
        "Gypsy or Irish Traveller": "White-Gypsy or Irish Traveller",
        "Arab": "Other ethnic group-Arab",
        "Any other white": "Any other white background",
    }
    df = update_column_values_from_map(df, "ethnicity", ethnicity_value_map)
    df = df.withColumn(
        "ethnicity",
        F.when(F.col("ethnicity").isNull(), "Any other ethnic group").otherwise(F.col("ethnicity")),
    )
    df = assign_column_from_mapped_list_key(
        df=df, column_name_to_assign="ethnicity_group", reference_column="ethnicity", map=ethnic_group_map
    )
    df = assign_ethnicity_white(
        df, column_name_to_assign="ethnicity_white", ethnicity_group_column_name="ethnicity_group"
    )
    return df


def fill_forwards_and_backwards(df: DataFrame):
    """"""
    df = fill_backwards_overriding_not_nulls(
        df=df,
        column_identity="participant_id",
        ordering_column="visit_datetime",
        dataset_column="survey_response_dataset_major_version",
        column_list=["sex", "date_of_birth", "ethnicity"],
    )
    df = fill_forward_only_to_nulls(
        df=df,
        id="participant_id",
        date="visit_datetime",
        list_fill_forward=[
            "sex",
            "date_of_birth",
            "ethnicity",
        ],
    )
    df = fill_forward_from_last_change(
        df=df,
        fill_forward_columns=[
            "been_outside_uk_last_country",
            "been_outside_uk_last_return_date",
            "been_outside_uk",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        record_changed_column="been_outside_uk",
        record_changed_value="Yes",
    )
    return df


# Transformations that require all data to be present in each row (as much as possible)
def derive_people_in_household_count(df) -> DataFrame:
    """
    Correct counts of household member groups and sum to get total number of people in household. Takes maximum
    final count by household for each record.

    Derived:
    - household_participant_count
    - household_participants_not_consenting_count
    - household_members_over_2_years_and_not_present_count
    - household_members_under_2_years_count
    - people_in_household_count
    - people_in_household_count_group

    Reference:
    - ons_household_id
    - participant_id
    - person_not_consenting_age_[1-9]
    - person_not_present_age_[1-8]
    - household_members_under_2_years
    """
    df = assign_household_participant_count(
        df,
        column_name_to_assign="household_participant_count",
        household_id_column="ons_household_id",
        participant_id_column="participant_id",
    )
    # df = update_person_count_from_ages(
    #     df,
    #     column_name_to_assign="household_participants_not_consenting_count",
    #     column_pattern=r"person_not_consenting_age_[1-9]",
    # )
    # df = update_person_count_from_ages(
    #     df,
    #     column_name_to_assign="household_members_over_2_years_and_not_present_count",
    #     column_pattern=r"person_not_present_age_[1-8]",
    # )
    # df = assign_household_under_2_count(
    #     df,
    #     column_name_to_assign="household_members_under_2_years_count",
    #     column_pattern=r"infant_age_months_[1-9]",
    #     condition_column="household_members_under_2_years",
    # )
    # household_window = Window.partitionBy("ons_household_id")

    # household_participants = [
    #     "household_participant_count",
    #     "household_participants_not_consenting_count",
    #     "household_members_over_2_years_and_not_present_count",
    #     "household_members_under_2_years_count",
    # ]
    # for household_participant_type in household_participants:
    #     df = df.withColumn(
    #         household_participant_type,
    #         F.max(household_participant_type).over(household_window),
    #     )
    # df = df.withColumn(
    #     "people_in_household_count",
    #     sum_within_row(household_participants),
    # )
    # df = df.withColumn(
    #     "people_in_household_count_group",
    #     F.when(F.col("people_in_household_count") >= 5, "5+").otherwise(
    #         F.col("people_in_household_count").cast("string")
    #     ),
    # )
    return df


def impute_key_columns(df: DataFrame, imputed_value_lookup_df: DataFrame, log_directory: str) -> DataFrame:
    """
    Impute missing values for key variables that are required for weight calibration.
    Most imputations require geographic data being joined onto the response records.

    Returns a single record per participant, with response values (when available) and missing values imputed.

    Edited:
    - ethnicity_white
    - sex
    - date_of_birth

    Reference:
    - participant_id
    - visit_datetime
    - cis_area_code_20
    - region_code
    - people_in_household_count_group
    - work_status_group
    """
    unique_id_column = "participant_id"

    # Get latest record for each participant, assumes that they have been filled forwards
    participant_window = Window.partitionBy(unique_id_column).orderBy(F.col("visit_datetime").desc())
    deduplicated_df = (
        df.withColumn("ROW_NUMBER", F.row_number().over(participant_window))
        .filter(F.col("ROW_NUMBER") == 1)
        .drop("ROW_NUMBER")
    )

    if imputed_value_lookup_df is not None:
        deduplicated_df = merge_previous_imputed_values(deduplicated_df, imputed_value_lookup_df, unique_id_column)

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_mode,
        reference_column="ethnicity_white",
        group_by_column="ons_household_id",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_by_k_nearest_neighbours,
        reference_column="ethnicity_white",
        donor_group_columns=["cis_area_code_20"],
        donor_group_column_weights=[5000],
        log_file_path=log_directory,
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        imputation_function=impute_by_distribution,
        reference_column="sex",
        group_by_columns=["ethnicity_white", "region_code"],
        first_imputation_value="Female",
        second_imputation_value="Male",
    ).custom_checkpoint()

    deduplicated_df = impute_and_flag(
        deduplicated_df,
        impute_date_by_k_nearest_neighbours,
        reference_column="date_of_birth",
        donor_group_columns=["region_code", "people_in_household_count_group", "work_status_group"],
        log_file_path=log_directory,
    )

    return deduplicated_df.select(
        unique_id_column,
        *["ethnicity_white", "sex", "date_of_birth"],
        *[col for col in deduplicated_df.columns if col.endswith("_imputation_method")],
        *[col for col in deduplicated_df.columns if col.endswith("_is_imputed")],
    )


def geography_dependent_transformations(
    df: DataFrame,
    imputed_demographic_columns_df: DataFrame,
):
    """
    Derived:
    - multigenerational_household
    - outward_postcode
    - age_at_visit
    - age_group_school_year

    Reference:
    - ons_household_id
    - participant_id
    - date_of_birth
    - country_name_12
    - school_year
    """
    df = df.drop(*[col for col in imputed_demographic_columns_df.columns if col != "participant_id"])

    df = df.join(imputed_demographic_columns_df, on="participant_id", how="left")  # join imputed data

    df = assign_outward_postcode(df, "outward_postcode", reference_column="postcode")

    df = assign_multigenerational(
        df=df,
        column_name_to_assign="multigenerational_household",
        participant_id_column="participant_id",
        household_id_column="ons_household_id",
        visit_date_column="visit_datetime",
        date_of_birth_column="date_of_birth",
        country_column="country_name_12",
    )  # Includes school year and age_at_visit derivations

    df = derive_age_based_columns(df, "age_at_visit")

    df = assign_age_group_school_year(
        df,
        country_column="country_name_12",
        age_column="age_at_visit",
        school_year_column="school_year",
        column_name_to_assign="age_group_school_year",
    )
    # df = create_formatted_datetime_string_columns(df)
    return df
