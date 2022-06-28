import pytest
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import transform_from_lookups


def test_transform_antibody_swab_ETL(spark_session):
    # df survey data
    cohort_lookup_schema = """
        participant_id string,
        old_cohort string,
        new_cohort string
    """
    cohort_lookup_data = [
        ("DHR-30021600042", "old", "new"),
        ("DHR-30084600055", "old", "new"),
        ("DHR-30007860069", "old", "new"),
        ("DHR-30026900012", "old", "new"),
        ("DHR-30042000042", "old", "new"),
    ]
    cohort_lookup = spark_session.createDataFrame(cohort_lookup_data, schema=cohort_lookup_schema)

    travel_lookup_schema = """
        been_outside_uk_last_country_old string,
        been_outside_uk_last_country_new string
    """
    travel_lookup_data = [
        ("13", None),
        ("0", None),
        ("23/07/2020", None),
        ("22/09/2020", None),
        ("!4DD", None),
    ]
    travel_lookup = spark_session.createDataFrame(travel_lookup_data, schema=travel_lookup_schema)

    tenure_lookup_schema = """
        UAC long,
        numAdult integer,
        numChild integer,
        dvhsize integer,
        tenure_group integer
    """
    tenure_lookup_data = [
        (10012345678, 1, 1, 1, 1),
        (10012346658, 1, 1, 1, 1),
        (10014342678, 1, 1, 1, 1),
    ]
    tenure_lookup = spark_session.createDataFrame(tenure_lookup_data, schema=tenure_lookup_schema)

    input_schema = """
        participant_id string,
        ons_household_id long,
        study_cohort string,
        been_outside_uk_last_country string
    """

    input_data = [
        ("DHR-30021600042", 10012345678, "old", "13"),
        ("DHR-30084600055", 10012346658, "old", "0"),
        ("DHR-30007860069", 10012345678, "old", "23/07/2020"),
        ("DHR-30026900012", 10014342678, "old", "22/09/2020"),
        ("DHR-30042000042", 10014342678, "old", "!4DD"),
        ("DHR-30042000055", 10014342678, "old", "England"),  # un modified country should be left unchanged by editing
        ("DHR-30069000069", None, None, None),  # null fields excluding participant id should result in all nulls
        (
            "DHR-30076000069",
            None,
            "old",
            None,
        ),  # un referenced participant in cohort should have null cohort_participant_id and unreferenced onshousehold_id
    ]
    input_df = spark_session.createDataFrame(input_data, schema=input_schema)

    expected_schema = """
        participant_id string,
        ons_household_id long,
        study_cohort string,
        been_outside_uk_last_country string,
        cohort_participant_id string,
        lfs_adults_in_household_count integer,
        lfs_children_in_household_count integer,
        lfs_people_in_household_count integer,
        lfs_tenure_group integer
    """
    expected_data = [
        ("DHR-30069000069", None, None, None, None, None, None, None, None),
        ("DHR-30076000069", None, "old", None, None, None, None, None, None),
        ("DHR-30021600042", 10012345678, "new", None, "DHR-30021600042", 1, 1, 1, 1),
        ("DHR-30007860069", 10012345678, "new", None, "DHR-30007860069", 1, 1, 1, 1),
        ("DHR-30084600055", 10012346658, "new", None, "DHR-30084600055", 1, 1, 1, 1),
        (
            "DHR-30026900012",
            10014342678,
            "new",
            None,
            "DHR-30026900012",
            1,
            1,
            1,
            1,
        ),  # un modified country should be left unchanged by editing
        (
            "DHR-30042000042",
            10014342678,
            "new",
            None,
            "DHR-30042000042",
            1,
            1,
            1,
            1,
        ),  # null fields excluding participant id should result in all nulls
        (
            "DHR-30042000055",
            10014342678,
            "old",
            "England",
            None,
            1,
            1,
            1,
            1,
        ),  # un referenced participant in cohort should have null cohort_participant_id
    ]
    expected_df = spark_session.createDataFrame(expected_data, schema=expected_schema)

    output_df = transform_from_lookups(input_df, cohort_lookup, travel_lookup, tenure_lookup)

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
