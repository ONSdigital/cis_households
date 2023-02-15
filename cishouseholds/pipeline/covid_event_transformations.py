from functools import reduce
from operator import add
from operator import and_

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_any_symptoms_around_visit
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_date_difference
from cishouseholds.derive import assign_grouped_variable_from_days_since
from cishouseholds.derive import assign_grouped_variable_from_days_since_contact
from cishouseholds.derive import assign_last_non_null_value_from_col_list
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import contact_known_or_suspected_covid_type
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.edit import conditionally_set_column_values
from cishouseholds.edit import correct_date_ranges_union_dependent
from cishouseholds.edit import normalise_think_had_covid_columns
from cishouseholds.edit import nullify_columns_before_date
from cishouseholds.edit import remove_incorrect_dates
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.edit import update_to_value_if_any_not_null
from cishouseholds.expressions import all_columns_values_in_list
from cishouseholds.expressions import any_column_equal_value
from cishouseholds.expressions import count_occurrence_in_row
from cishouseholds.impute import fill_forward_event
from cishouseholds.pipeline.mapping import date_cols_min_date_dict

# from cishouseholds.edit import fuzzy_update


def covid_event_transformations(df: DataFrame) -> DataFrame:
    """Apply all transformations related to covid event columns in order."""
    df = edit_existing_columns(df).custom_checkpoint()
    df = derive_new_columns(df).custom_checkpoint()
    df = fill_forward(df).custom_checkpoint()
    df = clean_inconsistent_event_detail_part_1(df).custom_checkpoint()  # a25 stata logic
    df = clean_inconsistent_event_detail_part_2(df).custom_checkpoint()  # a26 stata logic
    df = data_dependent_derivations(df).custom_checkpoint()
    return df


def edit_existing_columns(df: DataFrame) -> DataFrame:
    """
    Edited columns:
    - think_had_covid_onset_date
    - last_suspected_covid_contact_date
    - last_covid_contact_date
    - contact_suspected_positive_covid_last_28_days
    - contact_known_positive_covid_last_28_days
    - think_had_covid
    - think_had_covid_symptom_*
    - think_have_covid_onset_date
    - been_outside_uk_last_return_date
    - other_covid_infection_test_first_positive_date
    - other_covid_infection_test_last_negative_date
    - other_antibody_test_first_positive_date
    - other_antibody_test_last_negative_date

    Reference columns:
    - think_had_covid_admitted_to_hospital
    - think_had_covid_contacted_nhs
    - visit_datetime
    - visit_id
    - participant_id
    """

    df = normalise_think_had_covid_columns(df, "think_had_covid_symptom")

    invalid_covid_date = "2019-11-17"
    conditions = {
        "think_had_covid_onset_date": (
            (F.col("think_had_covid_onset_date").isNotNull())
            & (F.col("think_had_covid_onset_date") < invalid_covid_date)
        ),
        "last_suspected_covid_contact_date": (
            (F.col("last_suspected_covid_contact_date").isNotNull())
            & (F.col("last_suspected_covid_contact_date") < invalid_covid_date)
        ),
        "last_covid_contact_date": (
            (F.col("last_covid_contact_date").isNotNull()) & (F.col("last_covid_contact_date") < invalid_covid_date)
        ),
    }
    col_value_maps = {
        "think_had_covid_onset_date": {
            "think_had_covid_onset_date": None,
            "think_had_covid_contacted_nhs": None,
            "think_had_covid_admitted_to_hospital": None,
            "think_had_covid_symptom_": None,
        },
        "last_suspected_covid_contact_date": {
            "last_suspected_covid_": None,
            "think_had_covid_onset_date": None,
            "contact_suspected_positive_covid_last_28_days": "No",
        },
        "last_covid_contact_date": {
            "last_covid_": None,
            "think_had_covid_onset_date": None,
            "contact_known_positive_covid_last_28_days": "No",
        },
    }
    for condition in list(conditions.keys()):
        df = conditionally_set_column_values(
            df=df,
            condition=conditions.get(condition),
            cols_to_set_to_value=col_value_maps.get(condition),
        )

    df = df.custom_checkpoint()

    contact_dates = ["last_suspected_covid_contact_date", "last_covid_contact_date"]
    covid_contacts = ["contact_suspected_positive_covid_last_28_days", "contact_known_positive_covid_last_28_days"]
    contact_types = ["last_suspected_covid_contact_type", "last_covid_contact_type"]
    for contact_date, contact_type, contact in zip(contact_dates, contact_types, covid_contacts):
        # correct covid contact based on date
        df = update_to_value_if_any_not_null(
            df=df,
            column_name_to_update=contact,
            true_false_values=["Yes", "No"],
            column_list=[contact_date],
        )

        # correct covid type based on date
        df = update_to_value_if_any_not_null(
            df=df,
            column_name_to_update=contact_type,
            true_false_values=[F.col(contact_type), None],
            column_list=[contact_date],
        )

    df = df.withColumn(
        "think_had_covid",
        F.when(conditions.get("think_had_covid_onset_date"), "No").otherwise(F.col("think_had_covid")),
    )
    # think_had_covid_columns = [c for c in df.columns if c.startswith("think_had_covid_symptom_")]
    # df = fuzzy_update(
    #     df,
    #     id_column="participant_id",
    #     cols_to_check=[
    #         "other_covid_infection_test",
    #         "other_covid_infection_test_results",
    #         "think_had_covid_admitted_to_hospital",
    #         "think_had_covid_contacted_nhs",
    #         *think_had_covid_columns,
    #     ],
    #     update_column="think_had_covid_onset_date",
    #     min_matches=len(think_had_covid_columns),  # num available columns - (4 + date column itself)
    #     filter_out_of_range=True,
    # )

    date_cols_to_correct = [
        col
        for col in [
            "last_covid_contact_date",
            "last_suspected_covid_contact_date",
            "think_had_covid_onset_date",
            "think_have_covid_onset_date",
            "been_outside_uk_last_return_date",
            "other_covid_infection_test_first_positive_date",
            "other_covid_infection_test_last_negative_date",
            "other_antibody_test_first_positive_date",
            "other_antibody_test_last_negative_date",
        ]
        if col in df.columns
    ]
    df = correct_date_ranges_union_dependent(df, date_cols_to_correct, "participant_id", "visit_datetime", "visit_id")
    df = remove_incorrect_dates(df, date_cols_to_correct, "visit_datetime", "2019-08-01", date_cols_min_date_dict)

    return df


def derive_new_columns(df: DataFrame) -> DataFrame:
    """
    New columns:
    - days_since_think_had_covid
    - days_since_think_had_covid_group
    - think_have_covid_symptom_count
    - think_had_covid_symptom_count
    - think_have_covid_symptom_any
    - think_have_covid_cghfevamn_symptom_group
    - any_think_have_covid_symptom_or_now

    Reference columns:
    - think_had_covid_symptom_*
    """
    df = assign_date_difference(df, "days_since_think_had_covid", "think_had_covid_onset_date", "visit_datetime")

    df = assign_grouped_variable_from_days_since(
        df=df,
        binary_reference_column="think_had_covid",
        days_since_reference_column="days_since_think_had_covid",
        column_name_to_assign="days_since_think_had_covid_group",
    )
    original_think_have_symptoms = [
        "think_have_covid_symptom_fever",
        "think_have_covid_symptom_muscle_ache",
        "think_have_covid_symptom_fatigue",
        "think_have_covid_symptom_sore_throat",
        "think_have_covid_symptom_cough",
        "think_have_covid_symptom_shortness_of_breath",
        "think_have_covid_symptom_headache",
        "think_have_covid_symptom_nausea_or_vomiting",
        "think_have_covid_symptom_abdominal_pain",
        "think_have_covid_symptom_diarrhoea",
        "think_have_covid_symptom_loss_of_taste",
        "think_have_covid_symptom_loss_of_smell",
        "think_have_covid_symptom_more_trouble_sleeping",
        "think_have_covid_symptom_runny_nose_or_sneezing",
        "think_have_covid_symptom_noisy_breathing",
        "think_have_covid_symptom_loss_of_appetite",
    ]
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_have_covid_symptom_count",
        selection_columns=original_think_have_symptoms,
        count_if_value="Yes",
    )
    df = update_think_have_covid_symptom_any(
        df=df, column_name_to_update="think_have_covid_symptom_any", symptom_list=original_think_have_symptoms
    )
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_had_covid_symptom_count",
        selection_columns=[
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="any_think_have_covid_symptom_or_now",
        reference_columns=["think_have_covid_symptom_any", "think_have_covid"],
        true_false_values=["Yes", "No"],
    )

    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_have_covid_symptom_cough",
            "think_have_covid_symptom_fever",
            "think_have_covid_symptom_loss_of_smell",
            "think_have_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        id_column="participant_id",
        symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    return df


def fill_forward(df) -> DataFrame:
    """
    Function that contains all fill_forward_event calls required to implement STATA-based last observation carried forward logic.
    """
    # Derive these after fill forwards and other changes to dates
    df = fill_forward_event(
        df=df,
        event_indicator_column="contact_suspected_positive_covid_last_28_days",
        event_date_column="last_suspected_covid_contact_date",
        event_date_tolerance=7,
        detail_columns=["last_suspected_covid_contact_type"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = fill_forward_event(
        df=df,
        event_indicator_column="contact_known_positive_covid_last_28_days",
        event_date_column="last_covid_contact_date",
        event_date_tolerance=7,
        detail_columns=["last_covid_contact_type"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
    return df


def clean_inconsistent_event_detail_part_1(df: DataFrame) -> DataFrame:
    """
    Clean all variables related to the swab covid test.

    Edited columns:
     - think_had_covid
     - other_covid_infection_test_results
     - other_covid_infection_test
     - think_had_covid_contacted_nhs
     - think_had_covid_admitted_to_hospital
    """
    # Clean where date and/or symptoms are present, but ‘feeder question’ is no to thinking had covid.

    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid_onset_date").isNotNull()) | (F.col("think_had_covid_symptom_count") > 0), "Yes"
        ).otherwise(F.col("think_had_covid")),
    )
    df = df.withColumn(
        "other_covid_infection_test_results",
        F.when(
            (
                (F.col("other_covid_infection_test_results") == "Negative")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
            ),
            None,
        ).otherwise(F.col("other_covid_infection_test_results")),
    )

    # if the participant sais they have not had another covid test but there is a result for the test
    # and covid symptoms present or a date where symptoms occurred exists or the user has been involved with a hospital
    # due to covid then set to 'Yes'
    df = df.withColumn(
        "other_covid_infection_test",
        F.when(
            (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
            & (F.col("other_covid_infection_test_results").isNotNull())
            & (
                ((F.col("think_had_covid_symptom_count") > 0) | (F.col("think_had_covid_onset_date").isNotNull()))
                | (
                    (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                    & (F.col("think_had_covid_contacted_nhs") == "Yes")
                )
            ),
            "Yes",
        ).otherwise(F.col("other_covid_infection_test")),
    )
    df.cache()

    # Reset no (0) to missing where ‘No’ overall and random ‘No’s given for other covid variables.
    flag = (
        (F.col("think_had_covid_symptom_count") == 0)
        & (~F.col("other_covid_infection_test_results").eqNullSafe("Positive"))
        & reduce(
            and_,
            (
                (~F.col(c).eqNullSafe("Yes"))
                for c in [
                    "think_had_covid",
                    "think_had_covid_contacted_nhs",
                    "think_had_covid_admitted_to_hospital",
                    "other_covid_infection_test",
                ]
            ),
        )
    )
    for col in ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital"]:
        df = df.withColumn(col, F.when(flag, None).otherwise(F.col(col)))

    for col in ["other_covid_infection_test", "other_covid_infection_test_results"]:
        df = df.withColumn(
            col,
            F.when((flag) & (F.col("survey_response_dataset_major_version") == 0), None).otherwise(F.col(col)),
        )

    df.unpersist()
    df.cache()

    # Clean where admitted is 1 but no to ‘feeder question’ for v0 dataset.

    for col in ["think_had_covid", "think_had_covid_admitted_to_hospital"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (~F.col("think_had_covid_contacted_nhs").eqNullSafe("Yes"))
                & (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
                & (F.col("think_had_covid_symptom_count") == 0)
                & (~F.col("other_covid_infection_test_results").eqNullSafe("Positive")),
                "No",
            ).otherwise(F.col(col)),
        )

    for col in ["think_had_covid_admitted_to_hospital", "think_had_covid_contacted_nhs"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col("think_had_covid") == "No")
                & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (F.col("think_had_covid_contacted_nhs") == "Yes")
                & (~F.col("other_covid_infection_test").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 0),
                "No",
            ).otherwise(F.col(col)),
        )

    for col in ["think_had_covid_admitted_to_hospital", "other_covid_infection_test", "think_had_covid"]:
        df = df.withColumn(
            col,
            F.when(
                F.col("think_had_covid").isNull()
                & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
                & (~F.col("think_had_covid_contacted_nhs").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test") == "Yes")
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_had_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 0),
                "No",
            ).otherwise(F.col(col)),
        )

    # Clean where admitted is 1 but no to ‘feeder question’.

    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid") != "Yes")
            & (F.col("think_had_covid_admitted_to_hospital") == "Yes")
            & (F.col("think_had_covid_symptom_count") == 0)
            & (
                (F.col("think_had_covid_contacted_nhs") != "Yes")
                | (F.col("other_covid_infection_test_results").isNotNull())
            )
            & (F.col("other_covid_infection_test") == "Yes"),
            "Yes",
        ).otherwise(F.col("think_had_covid")),
    )
    df.unpersist()
    return df


def clean_inconsistent_event_detail_part_2(df) -> DataFrame:
    """
    Edited columns:
    - think_had_covid
    - think_had_covid_admitted_to_hospital
    - think_had_covid_contacted_nhs
    - other_covid_infection_test
    - survey_response_dataset_major_version
    - think_have_covid_symptom_count
    - think_had_covid_onset_date
    - other_covid_infection_test_results
    """
    think_had_covid_cols = [
        "survey_response_dataset_major_version",
        "think_had_covid_admitted_to_hospital",
        "think_had_covid",
        "think_had_covid_contacted_nhs",
        "think_have_covid_symptom_count",
        "think_had_covid_onset_date",
    ]
    hospital_covid_cols = ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital"]
    other_covid_test_cols = ["other_covid_infection_test", "other_covid_infection_test_results"]
    covid_test_cols = [*hospital_covid_cols, *other_covid_test_cols]
    # 1
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid_admitted_to_hospital",
        [
            [
                None,
                [1, "No", "No", "No", 0, None],
            ]
        ],
        think_had_covid_cols,
        override_original=False,
    )
    # 2
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid_contacted_nhs",
        [
            [
                None,
                [1, None, "No", "No", 0, None],
            ]
        ],
        think_had_covid_cols,
        override_original=False,
    )
    # 3
    df.cache()
    for col in ["think_had_covid_contacted_nhs", "think_had_covid_admitted_to_hospital", "other_covid_infection_test"]:
        df = df.withColumn(
            col,
            F.when(
                (~F.col("think_had_covid").eqNullSafe("Yes"))
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("survey_response_dataset_major_version") == 0)
                & (
                    reduce(
                        add,
                        [
                            F.when(F.col(c).isin(["Yes", "One or more positive test(s)"]), 1).otherwise(0)
                            for c in covid_test_cols
                        ],
                    )
                    == 1
                ),
                None,
            ).otherwise(F.col(col)),
        )
    # 4
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid",
        [
            [
                "No",
                [0, "No", None, ["No", None], 0, None, "Yes", "Any tests negative, but none positive"],
            ],
        ],
        [*think_had_covid_cols, "other_covid_infection_test", "other_covid_infection_test_results"],
        override_original=False,
    )
    # 5
    df = assign_column_value_from_multiple_column_map(
        df,
        "think_had_covid",
        [["Yes", ["No", "Yes", "One or more positive test(s)", 0]]],
        [
            "think_had_covid",
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 6
    for col in covid_test_cols:
        df = df.withColumn(
            col,
            F.when(
                (all_columns_values_in_list(covid_test_cols, ["No", "Any tests negative, but none positive", None]))
                & (F.col("think_had_covid") == "Yes")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("survey_response_dataset_major_version") == 0),
                None,
            ).otherwise(F.col(col)),
        )
    # 7
    df = assign_column_value_from_multiple_column_map(
        df,
        "other_covid_infection_test",
        [[None, ["No", "No", None, None, None, 0, 0]]],
        [
            "other_covid_infection_test",
            "think_had_covid",
            "think_had_covid_contacted_nhs",
            "think_had_covid_admitted_to_hospital",
            "other_covid_infection_test_results",
            "think_have_covid_symptom_count",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 8
    df.cache()
    for col in hospital_covid_cols:
        df = df.withColumn(
            col,
            F.when(
                (F.col(col) == "No")
                & (F.col("think_had_covid") == "No")
                & (F.col("think_had_covid_onset_date").isNull())
                & (F.col("think_have_covid_symptom_count") == 0)
                & (F.col("survey_response_dataset_major_version") == 1)
                & (
                    reduce(
                        add,
                        [
                            F.when(F.col(c).isin(["No", "Any tests negative, but none positive", None]), 1).otherwise(0)
                            for c in hospital_covid_cols
                        ],
                    )
                    == 1
                ),
                None,
            ).otherwise(F.col(col)),
        )
    # 9
    count_no = reduce(
        add,
        [
            *[F.when(F.col(c).eqNullSafe("No"), 1).otherwise(0) for c in hospital_covid_cols],
            F.when(
                F.col("other_covid_infection_test") == "No", F.col("survey_response_dataset_major_version")
            ).otherwise(0),
        ],
    )
    count_yes = count_occurrence_in_row([*hospital_covid_cols, "other_covid_infection_test"], "Yes")
    for col in [*hospital_covid_cols, "other_covid_infection_test"]:
        df = df.withColumn(
            col,
            F.when(
                (F.col(col).eqNullSafe("No"))
                & (F.col("think_had_covid").isNull())
                & (count_no <= 2)
                & (F.col("other_covid_infection_test_results").isNull())
                & (F.col("survey_response_dataset_major_version") == 0),
                None,
            ).otherwise(F.col(col)),
        )
    # 10
    df = assign_column_value_from_multiple_column_map(
        df,
        "other_covid_infection_test_results",
        [[None, ["Any tests negative, but none positive", None, None, None, None, "No", 0]]],
        [
            "other_covid_infection_test_results",
            "think_had_covid",
            "think_had_covid_onset_date",
            "think_had_covid_contacted_nhs",
            "think_had_covid_admitted_to_hospital",
            "other_covid_infection_test",
            "survey_response_dataset_major_version",
        ],
        override_original=False,
    )
    # 11
    df = df.withColumn(
        "think_had_covid",
        F.when(
            (F.col("think_had_covid").isNull())
            & (count_no >= 3)
            & (count_yes == 0)
            & (F.col("other_covid_infection_test_results").isNull())
            & (F.col("think_had_covid_onset_date").isNull())
            & (F.col("survey_response_dataset_major_version") == 0),
            "No",
        ).otherwise(F.col("think_had_covid")),
    )
    # 12
    for col in covid_test_cols:
        df = assign_column_value_from_multiple_column_map(
            df,
            col,
            [
                [
                    None,
                    [
                        ["Any tests negative, but none positive", None],
                        "No",
                        None,
                        [None, "No"],
                        [None, "No"],
                        [None, "No"],
                        0,
                        0,
                    ],
                ]
            ],
            [
                "other_covid_infection_test_results",
                "think_had_covid",
                "think_had_covid_onset_date",
                "think_had_covid_contacted_nhs",
                "think_had_covid_admitted_to_hospital",
                "other_covid_infection_test",
                "survey_response_dataset_major_version",
                "think_have_covid_symptom_count",
            ],
            override_original=False,
        )
    # 13
    for col in hospital_covid_cols:
        df = assign_column_value_from_multiple_column_map(
            df,
            col,
            [[None, [["Any tests negative, but none positive", None], "No", None, [None, "No"], "Yes", 0, 0]]],
            [
                "other_covid_infection_test_results",
                "think_had_covid",
                "think_had_covid_onset_date",
                "think_had_covid_admitted_to_hospital",
                "other_covid_infection_test",
                "survey_response_dataset_major_version",
                "think_have_covid_symptom_count",
            ],
            override_original=False,
        )
        df.unpersist()
    return df


def data_dependent_derivations(df: DataFrame) -> DataFrame:
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="any_symptoms_around_visit",
        symptoms_bool_column="any_think_have_covid_symptom_or_now",
        id_column="participant_id",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
        id_column="participant_id",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    df = derive_contact_any_covid_covid_variables(df)
    df = nullify_columns_before_date(
        df,
        column_list=[
            "think_had_covid_symptom_loss_of_appetite",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_more_trouble_sleeping",
        ],
        date_column="visit_datetime",
        date="2021-08-26",
    )
    df = nullify_columns_before_date(
        df,
        column_list=[
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_low_mood",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_anxiety",
        ],
        date_column="visit_datetime",
        date="2022-01-26",
    )
    return df


def derive_contact_any_covid_covid_variables(df: DataFrame) -> DataFrame:
    """
    Derive variables related to combination of know and suspected covid data columns.
    """
    df = df.withColumn(
        "contact_known_or_suspected_covid",
        F.when(
            any_column_equal_value(
                ["contact_suspected_positive_covid_last_28_days", "contact_known_positive_covid_last_28_days"], "Yes"
            ),
            "Yes",
        ).otherwise("No"),
    )

    df = assign_last_non_null_value_from_col_list(
        df=df,
        column_name_to_assign="contact_known_or_suspected_covid_latest_date",
        column_list=["last_covid_contact_date", "last_suspected_covid_contact_date"],
    )

    df = contact_known_or_suspected_covid_type(
        df=df,
        contact_known_covid_type_column="last_covid_contact_type",
        contact_suspect_covid_type_column="last_suspected_covid_contact_type",
        contact_any_covid_type_column="contact_known_or_suspected_covid",
        contact_any_covid_date_column="contact_known_or_suspected_covid_latest_date",
        contact_known_covid_date_column="last_covid_contact_date",
        contact_suspect_covid_date_column="last_suspected_covid_contact_date",
    )

    df = assign_date_difference(
        df,
        "contact_known_or_suspected_covid_days_since",
        "contact_known_or_suspected_covid_latest_date",
        "visit_datetime",
    )

    df = assign_grouped_variable_from_days_since_contact(
        df=df,
        reference_column="contact_known_or_suspected_covid",
        days_since_reference_column="contact_known_or_suspected_covid_days_since",
        column_name_to_assign="contact_known_or_suspected_covid_days_since_group",
    )

    return df
