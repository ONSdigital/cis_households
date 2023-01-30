from functools import reduce
from operator import add
from operator import and_

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.derive import assign_any_symptoms_around_visit
from cishouseholds.derive import assign_column_value_from_multiple_column_map
from cishouseholds.derive import assign_true_if_any
from cishouseholds.derive import count_value_occurrences_in_column_subset_row_wise
from cishouseholds.edit import fuzzy_update
from cishouseholds.edit import update_think_have_covid_symptom_any
from cishouseholds.expressions import all_columns_values_in_list
from cishouseholds.expressions import count_occurrence_in_row
from cishouseholds.impute import fill_forward_event
from cishouseholds.pipeline.post_union_transformations import derive_contact_any_covid_covid_variables


def symptom_transformations(df: DataFrame) -> DataFrame:
    """Apply all transformations related to symptom columns in order."""
    df = symptom_column_transformations(df).custom_checkpoint()
    df = clean_covid_event_detail_cols(df).custom_checkpoint()  # a26 stata logic
    df = clean_covid_test_swab(df).custom_checkpoint()  # a25 stata logic
    df = fill_forward(df).custom_checkpoint()
    df = data_dependent_transformations(df).custom_checkpoint()
    return df


def symptom_column_transformations(df: DataFrame) -> DataFrame:
    df = count_value_occurrences_in_column_subset_row_wise(
        df=df,
        column_name_to_assign="think_have_covid_symptom_count",
        selection_columns=[
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
        ],
        count_if_value="Yes",
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
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_anxiety",
            "think_had_covid_symptom_low_mood",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_loss_of_appetite",
        ],
        count_if_value="Yes",
    )
    df = update_think_have_covid_symptom_any(
        df=df,
        column_name_to_update="think_have_covid_symptom_any",
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
    df = assign_true_if_any(
        df=df,
        column_name_to_assign="think_had_covid_cghfevamn_symptom_group",
        reference_columns=[
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_loss_of_taste",
        ],
        true_false_values=["Yes", "No"],
    )
    # df = assign_true_if_any(
    #     df=df,
    #     column_name_to_assign="think_have_covid_cghfevamn_symptom_group",
    #     reference_columns=[
    #         "think_had_covid_symptom_cough",
    #         "think_had_covid_symptom_fever",
    #         "think_had_covid_symptom_loss_of_smell",
    #         "think_had_covid_symptom_loss_of_taste",
    #     ],
    #     true_false_values=["Yes", "No"],
    # )
    df = assign_any_symptoms_around_visit(
        df=df,
        column_name_to_assign="symptoms_around_cghfevamn_symptom_group",
        id_column="participant_id",
        symptoms_bool_column="think_have_covid_cghfevamn_symptom_group",
        visit_date_column="visit_datetime",
        visit_id_column="visit_id",
    )
    think_had_covid_columns = [c for c in df.columns if c.startswith("think_had_covid_symptom_")]
    df = fuzzy_update(
        df,
        id_column="participant_id",
        cols_to_check=[
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "think_had_covid_admitted_to_hospital",
            "think_had_covid_contacted_nhs",
            *think_had_covid_columns,
        ],
        update_column="think_had_covid_onset_date",
        min_matches=len(think_had_covid_columns),  # num available columns - (4 + date column itself)
        right_df_filter=F.col("think_had_covid_onset_date") > F.col("visit_datetimeg"),
    )
    return df


def clean_covid_event_detail_cols(df) -> DataFrame:
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


def clean_covid_test_swab(df: DataFrame) -> DataFrame:
    """
    Clean all variables related to the swab covid test.
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


def fill_forward(df) -> DataFrame:
    """
    Function that contains all fill_forward_event calls required to implement STATA-based last observation carried forward logic.
    """
    df = fill_forward_event(
        df=df,
        event_indicator_column="think_had_covid",
        event_date_column="think_had_covid_onset_date",
        event_date_tolerance=7,
        detail_columns=[
            "other_covid_infection_test",
            "other_covid_infection_test_results",
            "think_had_covid_any_symptoms",
            "think_had_covid_admitted_to_hospital",
            "think_had_covid_contacted_nhs",
            "think_had_covid_symptom_fever",
            "think_had_covid_symptom_muscle_ache",
            "think_had_covid_symptom_fatigue",
            "think_had_covid_symptom_sore_throat",
            "think_had_covid_symptom_cough",
            "think_had_covid_symptom_shortness_of_breath",
            "think_had_covid_symptom_headache",
            "think_had_covid_symptom_nausea_or_vomiting",
            "think_had_covid_symptom_abdominal_pain",
            "think_had_covid_symptom_loss_of_appetite",
            "think_had_covid_symptom_noisy_breathing",
            "think_had_covid_symptom_runny_nose_or_sneezing",
            "think_had_covid_symptom_more_trouble_sleeping",
            "think_had_covid_symptom_diarrhoea",
            "think_had_covid_symptom_loss_of_taste",
            "think_had_covid_symptom_loss_of_smell",
            "think_had_covid_symptom_memory_loss_or_confusion",
            "think_had_covid_symptom_chest_pain",
            "think_had_covid_symptom_vertigo_or_dizziness",
            "think_had_covid_symptom_difficulty_concentrating",
            "think_had_covid_symptom_anxiety",
            "think_had_covid_symptom_palpitations",
            "think_had_covid_symptom_low_mood",
        ],
        participant_id_column="participant_id",
        visit_datetime_column="visit_datetime",
        visit_id_column="visit_id",
    )
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


def data_dependent_transformations(df: DataFrame) -> DataFrame:
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
    return df
