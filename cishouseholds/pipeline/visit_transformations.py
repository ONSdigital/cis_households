import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_given_proportion
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_ever_had_long_term_health_condition_or_disabled
from cishouseholds.derive import assign_first_visit
from cishouseholds.derive import assign_last_visit
from cishouseholds.derive import assign_named_buckets
from cishouseholds.derive import assign_visit_order
from cishouseholds.derive import derive_digital_merge_type
from cishouseholds.edit import replace_sample_barcode


def visit_transformations(df: DataFrame):
    """"""
    df = visit_derivations(df).custom_checkpoint()
    df = create_ever_variable_columns(df).custom_checkpoint()
    return df


def visit_derivations(df: DataFrame):

    df = assign_visit_order(
        df=df,
        column_name_to_assign="visit_order",
        id="participant_id",
        order_list=["visit_datetime", "visit_id"],
    )
    df = df.withColumn(
        "participant_visit_status", F.coalesce(F.col("participant_visit_status"), F.col("survey_completion_status"))
    )

    df = derive_digital_merge_type(df=df, column_name_to_assign="digital_merge_type")
    df = replace_sample_barcode(df=df)

    df = assign_first_visit(
        df=df,
        column_name_to_assign="household_first_visit_datetime",
        id_column="ons_household_id",
        visit_date_column="visit_datetime",
    )
    df = assign_last_visit(
        df=df,
        column_name_to_assign="last_attended_visit_datetime",
        id_column="ons_household_id",
        visit_status_column="participant_visit_status",
        visit_date_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="days_since_enrolment",
        start_reference_column="household_first_visit_datetime",
        end_reference_column="visit_datetime",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="fortnight_since_enrolment",
        start_reference_column="household_first_visit_datetime",
        end_reference_column="visit_datetime",
        format="fortnight",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="fortnight_of_enrolment",
        start_reference_column=F.lit("2020-04-16 00:00:00"),  # first fortnight of survey
        end_reference_column="household_first_visit_datetime",
        format="fortnight",
    )
    df = assign_date_difference(
        df=df,
        column_name_to_assign="household_weeks_since_survey_enrolment",
        start_reference_column="survey start",
        end_reference_column="visit_datetime",
        format="weeks",
    )
    df = assign_named_buckets(
        df,
        reference_column="days_since_enrolment",
        column_name_to_assign="visit_number",
        map={**{0: 1, 14: 2, 21: 3, 28: 4}, **{i * 28: (i + 3) for i in range(2, 200)}},
    )
    return df


def create_ever_variable_columns(df: DataFrame) -> DataFrame:
    """"""
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_work_person_facing_or_social_care",
        groupby_column="participant_id",
        reference_columns=["work_social_care"],
        count_if=["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing", "Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_care_home_worker",
        groupby_column="participant_id",
        reference_columns=["work_social_care", "work_nursing_or_residential_care_home"],
        count_if=["Yes", "Yes, care/residential home, resident-facing"],
        true_false_values=["Yes", "No"],
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition",
        groupby_column="participant_id",
        reference_columns=["illness_lasting_over_12_months"],
        count_if=["Yes"],
        true_false_values=["Yes", "No"],
    )
    df = assign_ever_had_long_term_health_condition_or_disabled(
        df=df,
        column_name_to_assign="ever_had_long_term_health_condition_or_disabled",
        health_conditions_column="illness_lasting_over_12_months",
        condition_impact_column="illness_reduces_activity_or_ability",
    )
    return df
