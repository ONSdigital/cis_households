import re
from functools import reduce
from itertools import chain
from operator import add
from typing import List
from typing import Optional
from typing import Union

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.expressions import all_equal
from cishouseholds.pyspark_utils import get_or_create_spark_session


def assign_multigeneration(
    df: DataFrame,
    column_name_to_assign: str,
    participant_id_column,
    household_id_column: str,
    visit_date_column: str,
    date_of_birth_column: str,
    country_column: str,
):
    """
    Assign a column to specify if a given household is multigeneration at the time one of its participants visited.
    Note: school year lookup dataframe must be amended to account for changes in school year start dates
    Parameters
    ----------
    df
    column_name_to_assign
    participant_id_column
    household_id_column
    visit_date_column
    date_of_birth_column
    country_column
    """
    spark_session = get_or_create_spark_session()
    school_year_lookup_df = spark_session.createDataFrame(
        data=[
            ("England", "09", "01", "09", "01"),
            ("Wales", "09", "01", "09", "01"),
            ("Scotland", "08", "15", "03", "01"),
            ("NI", "09", "01", "07", "02"),
        ],
        schema=[
            country_column,
            "school_start_month",
            "school_start_day",
            "school_year_ref_month",
            "school_year_ref_day",
        ],
    )
    transformed_df = df.groupBy(household_id_column, visit_date_column).count()
    transformed_df = transformed_df.join(
        df.select(household_id_column, participant_id_column, date_of_birth_column, country_column),
        on=household_id_column,
    ).drop("count")
    transformed_df = assign_age_at_date(
        df=transformed_df,
        column_name_to_assign="age_at_visit",
        base_date=F.col(visit_date_column),
        date_of_birth=F.col(date_of_birth_column),
    )
    transformed_df = assign_school_year(
        df=transformed_df,
        column_name_to_assign="school_year",
        reference_date_column=visit_date_column,
        dob_column=date_of_birth_column,
        country_column=country_column,
        school_year_lookup=school_year_lookup_df,
    )
    generation1_flag = F.when((F.col("age_at_visit") > 49), 1).otherwise(0)
    generation2_flag = F.when(
        ((F.col("age_at_visit") <= 49) & (F.col("age_at_visit") >= 17)) | (F.col("school_year") >= 12), 1
    ).otherwise(0)
    generation3_flag = F.when((F.col("school_year") <= 11), 1).otherwise(0)

    window = Window.partitionBy(household_id_column, visit_date_column)
    gen1_exists = F.when(F.sum(generation1_flag).over(window) >= 1, True).otherwise(False)
    gen2_exists = F.when(F.sum(generation2_flag).over(window) >= 1, True).otherwise(False)
    gen3_exists = F.when(F.sum(generation3_flag).over(window) >= 1, True).otherwise(False)

    transformed_df = transformed_df.withColumn(
        column_name_to_assign, F.when((gen1_exists) & (gen2_exists) & (gen3_exists), 1).otherwise(0)
    )
    transformed_df = (
        df.drop("age_at_visit")
        .join(
            transformed_df.select(
                "age_at_visit", "school_year", column_name_to_assign, participant_id_column, visit_date_column
            ),
            on=[participant_id_column, visit_date_column],
            how="left",
        )
        .distinct()
    )
    return transformed_df


def assign_household_participant_count(
    df: DataFrame, column_name_to_assign: str, household_id_column: str, participant_id_column: str
):
    """Assign the count of participants within each household."""
    household_window = Window.partitionBy(household_id_column)
    df = df.withColumn(
        column_name_to_assign, F.size(F.collect_set(F.col(participant_id_column)).over(household_window))
    )
    return df


def assign_household_under_2_count(df, column_name_to_assign: str, column_pattern: str):
    """Count number of individuals below two from age (months) columns matching pattern."""
    columns_to_count = [column for column in df.columns if re.match(column_pattern, column)]
    count = reduce(
        add, [F.when((F.col(column) >= 0) & (F.col(column) <= 24), 1).otherwise(0) for column in columns_to_count]
    )
    df = df.withColumn(column_name_to_assign, F.when(~all_equal(columns_to_count, 0), count).otherwise(0))
    return df


def assign_ever_had_long_term_health_condition_or_disabled(
    df: DataFrame, column_name_to_assign: str, health_conditions_column: str, condition_impact_column: str
):
    """
    Assign a column that identifies if patient is long term disabled by applying several
    preset functions
    Parameters
    ----------
    df
    column_name_to_assign
    health_conditions_column
    condition_impact_column
    """

    df = df.withColumn(
        "TEMP_EVERNEVER",
        F.when(
            (F.col(health_conditions_column) == "Yes")
            & (F.col(condition_impact_column).isin(["Yes, a little", "Yes, a lot"])),
            "Yes",
        )
        .when(
            (F.col(health_conditions_column).isin(["Yes", "No"]))
            & ((F.col(condition_impact_column) == "Not at all") | (F.col(condition_impact_column).isNull())),
            "No",
        )
        .otherwise(None),
    )
    df = assign_column_given_proportion(
        df=df,
        column_name_to_assign=column_name_to_assign,
        groupby_column="participant_id",
        reference_columns=["TEMP_EVERNEVER"],
        count_if=["Yes"],
        true_false_values=["Yes", "No"],
    )  # not sure of correct  PIPELINE categories

    return df.drop("TEMP_EVERNEVER")


def assign_random_day_in_month(
    df: DataFrame, column_name_to_assign: str, month_column: str, year_column: str
) -> DataFrame:
    """
    Assign a random date in a given year and month
    Parameters
    ----------
    month_column
    year_column
    """
    df = df.withColumn("TEMP_DAY", F.lit(1))
    df = df.withColumn("TEMP_DATE", F.concat_ws("-", year_column, month_column, "TEMP_DAY"))
    df = df.withColumn("TEMP_DAY", F.round(F.rand() * (F.date_format(F.last_day("TEMP_DATE"), "d") - 0.5001), 0) + 0.5)
    df = df.withColumn(
        column_name_to_assign,
        F.to_timestamp(F.concat_ws("-", year_column, month_column, F.ceil("TEMP_DAY")), format="yyyy-MM-dd"),
    )
    return df.drop("TEMP_DATE", "TEMP_DAY")


def assign_household_size(
    df: DataFrame,
    column_name_to_assign: str,
    household_participant_count_column: Optional[str] = None,
    existing_group_column: Optional[str] = None,
) -> DataFrame:
    """
    Assign household group size (grouped above 5+), using participant count to impute values missing in existing group
    column.
    """
    return df.withColumn(
        column_name_to_assign,
        F.coalesce(
            F.col(existing_group_column),
            F.when(F.col(household_participant_count_column) < 5, F.col(household_participant_count_column))
            .otherwise("5+")
            .cast("string"),
        ),
    )


def assign_first_visit(df: DataFrame, column_name_to_assign: str, id_column: str, visit_date_column: str) -> DataFrame:
    """
    Assign column to represent number of participants in household
    Parameters
    ----------
    df
    column_name_to_assign
    id_column
    visit_date_column
    """
    window = Window.partitionBy(id_column).orderBy(visit_date_column)
    return df.withColumn(column_name_to_assign, F.first(visit_date_column).over(window))


def assign_last_visit(
    df: DataFrame, column_name_to_assign: str, id_column: str, visit_date_column: str, visit_status_column: str
) -> DataFrame:
    """
    Assign a column to contain only the last date a participant completed a visited
    Parameters
    ----------
    id_column
    visit_date_column
    visit_status_column
    """
    window = Window.partitionBy(id_column).orderBy(F.desc(visit_date_column))
    df = df.withColumn(
        column_name_to_assign,
        F.first(
            F.when(~F.col(visit_status_column).isin("Cancelled", "Patient did not attend"), F.col(visit_date_column)),
            ignorenulls=True,
        ).over(window),
    )
    return df


def assign_column_given_proportion(
    df: DataFrame,
    column_name_to_assign: str,
    groupby_column: str,
    reference_columns: List[str],
    count_if: List[Union[str, int]],
    true_false_values: List[Union[str, int]],
) -> DataFrame:
    """
    Assign a column boolean 1, 0 when the proportion of values meeting a condition is above 0.3
    """
    window = Window.partitionBy(groupby_column)

    df = df.withColumn("TEMP", F.lit(0))
    df = df.withColumn(column_name_to_assign, F.lit("No"))

    for col in reference_columns:
        df = df.withColumn(
            "TEMP",
            F.when(F.col(col).isin(count_if), 1).when(F.col(col).isNotNull(), F.col("TEMP")).otherwise(None),
        )
        df = df.withColumn(
            column_name_to_assign,
            F.when(
                (
                    F.sum(F.when(F.col("TEMP") == 1, 1).otherwise(0)).over(window)
                    / F.sum(F.when(F.col("TEMP").isNotNull(), 1).otherwise(0)).over(window)
                    >= 0.3
                ),
                1,
            ).otherwise(0),
        )
    df = df.withColumn(column_name_to_assign, F.max(column_name_to_assign).over(window))
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(column_name_to_assign) == 1, true_false_values[0]).otherwise(true_false_values[1]),
    )
    return df.drop("TEMP")


def count_value_occurrences_in_column_subset_row_wise(
    df: DataFrame, column_name_to_assign: str, selection_columns: List[str], count_if_value: Union[str, int]
) -> DataFrame:
    """
    Assign a column to be the count of cells in selection row where condition is true
    Parameters
    ---------
    df
    column_name_to_assign
    selection_columns
    count_if_value
    """
    df = (
        df.withColumn(column_name_to_assign, F.array([F.col(col) for col in selection_columns]))
        .withColumn(column_name_to_assign, F.array_remove(column_name_to_assign, count_if_value))
        .withColumn(column_name_to_assign, F.lit(len(selection_columns) - F.size(F.col(column_name_to_assign))))
    )
    return df


def assign_any_symptoms_around_visit(
    df: DataFrame,
    column_name_to_assign: str,
    symptoms_bool_column: str,
    id_column: str,
    visit_date_column: str,
    visit_id_column: str,
) -> DataFrame:
    """
    Assign a column with boolean (Yes, No) if sympoms present around visit, derived
    from if symtoms bool columns reported any true values -1 +1 from time window
    """
    window = Window.partitionBy(id_column).orderBy(visit_date_column, visit_id_column)
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(symptoms_bool_column) == "Yes")
            | (F.lag(symptoms_bool_column, 1).over(window) == "Yes")
            | (F.lag(symptoms_bool_column, -1).over(window) == "Yes"),
            "Yes",
        ).otherwise("No"),
    )
    return df


def assign_true_if_any(
    df: DataFrame,
    column_name_to_assign: str,
    reference_columns: List[str],
    true_false_values: List[Union[str, int, bool]],
    ignore_nulls: Optional[bool] = False,
) -> DataFrame:
    """
    Assign column the second value of a list containing values for false and true
    if either of a list of reference columns are true
    column_name_to_assign is assigned initially to all false then assigned true when value found
    in any of the reference columns
    """
    df = df.withColumn(column_name_to_assign, F.lit(true_false_values[1]))
    for col in reference_columns:
        df = df.withColumn(
            column_name_to_assign,
            F.when(
                F.col(col).eqNullSafe(true_false_values[0]),
                true_false_values[0],
            )
            .when(F.col(col).isNull() & F.lit(ignore_nulls), None)
            .otherwise(F.col(column_name_to_assign)),
        )
    return df


def assign_proportion_column(
    df: DataFrame, column_name_to_assign: str, numerator_column: str, denominator_column: str, numerator_selector: str
) -> DataFrame:
    """
    Assign a column as the result of a division operation on total of select values from numerator column
    divided by grouped by another selector
    Parameters
    ----------
    df
    numerator_column
    denominator_column
    numerator_selector
    denominator_selector
    """
    window = Window.partitionBy(denominator_column)

    return df.withColumn(
        column_name_to_assign,
        (F.sum(F.when(F.col(numerator_column) == numerator_selector, 1).otherwise(0)).over(window))
        / (F.count(denominator_column).over(window)),
    )


def assign_work_social_column(
    df: DataFrame,
    column_name_to_assign: str,
    work_sector_column: str,
    care_home_column: str,
    direct_contact_column: str,
) -> DataFrame:
    """
    Assign column for work social with standard string values depending on 3 given reference inputs
    Parameters
    ----------
    df
    column_name_to_assign
    work_sector_column
    care_home_column
    direct_contact_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(work_sector_column).isNull(), None)
        .when(
            ~F.col(work_sector_column).isin(["Furloughed (temporarily not working)", "Social care", "Social Care"]),
            "No",
        )
        .when(
            (~F.col(work_sector_column).isin(["Social care", "Social Care"]))
            & (F.col(care_home_column).isNull())
            & (F.col(direct_contact_column).isNull()),
            None,
        )
        .when(
            (F.col(care_home_column) == "Yes") & (F.col(direct_contact_column) == "Yes"),
            "Yes, care/residential home, resident-facing",
        )
        .when(
            ((F.col(care_home_column) == "No") | (F.col(care_home_column).isNull()))
            & (F.col(direct_contact_column) == "Yes"),
            "Yes, other social care, resident-facing",
        )
        .when(
            ((F.col(direct_contact_column) == "No") | (F.col(direct_contact_column).isNull()))
            & (F.col(care_home_column) == "Yes"),
            "Yes, care/residential home, non-resident-facing",
        )
        .when(
            ((F.col(care_home_column) == "No") | (F.col(care_home_column).isNull()))
            & ((F.col(direct_contact_column) == "No") | (F.col(direct_contact_column).isNull())),
            "Yes, other social care, non-resident-facing",
        )
        .otherwise("No"),
    )
    return df


def assign_unique_id_column(df: DataFrame, column_name_to_assign: str, concat_columns: List[str]) -> DataFrame:
    """
    Assign a unique column from concatenating multiple input columns
    Parameters
    ----------
    concat_columns
    """
    return df.withColumn(column_name_to_assign, F.concat_ws("-", *concat_columns))


def assign_has_been_to_column(
    df: DataFrame, column_name_to_assign: str, contact_participant_column: str, contact_other_column: str
) -> DataFrame:
    """
    Assign a column to evidence whether a relevant party has been to a given place using the 2 input
    contact columns as reference and standardized output string column values
    Parameters
    ----------
    df
    column_name_to_assign
    contact_participant_column
    contact_other_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(contact_participant_column) == "No") & (F.col(contact_other_column) == "No"),
            "No, no one in my household has",
        )
        .when(F.col(contact_participant_column) == "Yes", "Yes, I have")
        .when(
            (F.col(contact_participant_column) == "No") & (F.col(contact_other_column) == "Yes"),
            "No I haven't, but someone else in my household has",
        )
        .otherwise(None),
    )
    return df


def assign_covid_contact_status(
    df: DataFrame, column_name_to_assign: str, known_column: str, suspect_column: str
) -> DataFrame:
    """
    Assign column for possibility of having covid-19
    Parameters
    ----------
    known_column
    suspect_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col(known_column) == "Yes") | (F.col(suspect_column) == "Yes"), "Yes").otherwise("No"),
    )
    return df


def assign_filename_column(df: DataFrame, column_name_to_assign: str) -> DataFrame:
    """
    Use inbuilt pyspark function to get name of the file used in the current spark task
    Regular expression removes unnecessary characters to allow checks for processed files
    Parameters
    ----------
    df
    column_name_to_assign
    """
    return df.withColumn(
        column_name_to_assign, F.regexp_replace(F.input_file_name(), r"(?<=:\/{2})(\w+|\d+)(?=\/{1})", "")
    )


def assign_column_from_mapped_list_key(
    df: DataFrame, column_name_to_assign: str, reference_column: str, map: dict
) -> DataFrame:
    """
    Assing a specific column value using a dictionary of values to assign as keys and
    the list criteria corresponding to when that value should be assign as a value
    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    map
    """
    df = df.withColumn(column_name_to_assign, F.lit(None))
    for val, key_list in map.items():
        df = df.withColumn(
            column_name_to_assign,
            F.when(F.col(reference_column).isin(*key_list), val).otherwise(F.col(column_name_to_assign)),
        )
    return df


def assign_test_target(df: DataFrame, column_name_to_assign: str, filename_column: str) -> DataFrame:
    """
    Assign a column for the appropriate test target type corresponding
    to that contained within the filename column (S, N)
    of visit
    Parameters
    ----------
    df
    column_name_to_assign
    filename_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(filename_column).contains("S"), "S")
        .when(F.col(filename_column).contains("N"), "N")
        .otherwise(None),
    )
    return df


def assign_school_year_september_start(
    df: DataFrame, dob_column: str, visit_date_column: str, column_name_to_assign: str
) -> DataFrame:
    """
    Assign a column for the approximate school year of an individual given their age at the time
    of visit
    Parameters
    ----------
    df
    age_column
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.month(F.col(visit_date_column))) >= 9) & ((F.month(F.col(dob_column))) < 9),
            (F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 3,
        )
        .when(
            (F.month(F.col(visit_date_column)) >= 9) | ((F.month(F.col(dob_column))) >= 9),
            (F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 4,
        )
        .otherwise((F.year(F.col(visit_date_column))) - (F.year(F.col(dob_column))) - 5),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col(column_name_to_assign) <= 0) | (F.col(column_name_to_assign) > 13), None).otherwise(
            F.col(column_name_to_assign)
        ),
    )
    return df


def assign_work_patient_facing_now(
    df: DataFrame, column_name_to_assign: str, age_column: str, work_healthcare_column: str
) -> DataFrame:
    """
    Assign column for work person facing depending on values of given input reference
    columns mapped to a list of outputs
    Parameters
    ----------
    df
    column_name_to_assign
    age_column
    work_healthcare_column
    """
    df = assign_column_from_mapped_list_key(
        df,
        column_name_to_assign,
        work_healthcare_column,
        {
            "Yes": [
                "Yes",
                "Yes, primary care, patient-facing",
                "Yes, secondary care, patient-facing",
                "Yes, other healthcare, patient-facing",
            ],
            "No": [
                "No",
                "Yes, primary care, non-patient-facing",
                "Yes, secondary care, non-patient-facing",
                "Yes, other healthcare, non-patient-facing",
            ],
        },
    )
    df = assign_named_buckets(
        df, age_column, column_name_to_assign, {0: "<=15y", 16: F.col(column_name_to_assign), 75: ">=75y"}
    )
    return df


def assign_work_person_facing_now(
    df: DataFrame,
    column_name_to_assign: str,
    work_patient_facing_now_column: str,
    work_social_care_column: str,
) -> DataFrame:
    """
    Assign column for work patient facing depending on values of given input reference
    columns mapped to a list of outputs
    Parameters
    ----------
    df
    work_patient_facing_now_column
    work_social_care_column
    column_name_to_assign
    """
    df = assign_column_from_mapped_list_key(
        df,
        column_name_to_assign,
        work_social_care_column,
        {
            "Yes": ["Yes, care/residential home, resident-facing", "Yes, other social care, resident-facing"],
            "No": [
                "No",
                "Yes, care/residential home, non-resident-facing",
                "Yes, other social care, non-resident-facing",
            ],
        },
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col(work_patient_facing_now_column) == "Yes", "Yes")
        .when(
            ~(F.col(work_patient_facing_now_column).isin("Yes", "No") | F.col(work_patient_facing_now_column).isNull()),
            F.col(work_patient_facing_now_column),
        )
        .otherwise(F.col(column_name_to_assign)),
    )
    return df


def assign_named_buckets(
    df: DataFrame, reference_column: str, column_name_to_assign: str, map: dict, use_current_values=False
) -> DataFrame:
    """
    Assign a new column with named ranges for given integer ranges contianed within a reference column
    Parameters
    ----------
    df
    reference_column
    column_name_to_assign
    map
        dictionary containing the map of minimum value in given range (inclusive) to range label string
    use_current_values
        boolean operation preset to False to specify if current values in column_name_to_assign should be carried
        forward if not in range of lookup buckets specified in map
    """
    bucketizer = Bucketizer(
        splits=[float("-Inf"), *list(map.keys()), float("Inf")], inputCol=reference_column, outputCol="buckets"
    )
    dfb = bucketizer.setHandleInvalid("keep").transform(df)

    bucket_dic = {0.0: F.col(column_name_to_assign) if use_current_values else None}
    for i, value in enumerate(map.values()):
        bucket_dic[float(i + 1)] = value

    mapping_expr = F.create_map([F.lit(x) for x in chain(*bucket_dic.items())])  # type: ignore

    dfb = dfb.withColumn(column_name_to_assign, mapping_expr[dfb["buckets"]])
    return dfb.drop("buckets")


def assign_age_group_school_year(
    df: DataFrame, country_column: str, age_column: str, school_year_column: str, column_name_to_assign: str
) -> DataFrame:
    """
    Assign column_age_group_school_year using multiple references column values in a specific pattern
    to determin a string coded representation of school year
    Parameters
    ----------
    df:
    country_column
    age_column
    school_year_column
    column_name_to_assign
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col(age_column) >= 2) & (F.col(age_column) <= 12) & (F.col(school_year_column) <= 6), "02-6SY")
        .when(
            ((F.col(school_year_column) >= 7) & (F.col(school_year_column) <= 11))
            | (
                (F.col(country_column).isin("England", "Wales"))
                & ((F.col(age_column) >= 12) & (F.col(age_column) <= 15))
                & (((F.col(school_year_column) <= 6) | (F.col(school_year_column).isNull())))
            )
            | (
                (F.col(country_column).isin("Scotland", "NI"))
                & ((F.col(age_column) >= 12) & (F.col(age_column) <= 14))
                & (((F.col(school_year_column) <= 6) | (F.col(school_year_column).isNull())))
            ),
            "07SY-11SY",
        )
        .when(
            (
                (F.col(country_column).isin("England", "Wales"))
                & ((F.col(age_column) >= 16) & (F.col(age_column) <= 24))
                & (F.col(school_year_column) >= 12)
            )
            | (
                (F.col(country_column).isin("Scotland", "NI"))
                & ((F.col(age_column) >= 15) & (F.col(age_column) <= 24))
                & (F.col(school_year_column) >= 12)
            ),
            "12SY-24",
        )
        .otherwise(None),
    )
    df = assign_named_buckets(
        df, age_column, column_name_to_assign, {25: "25-34", 35: "35-49", 50: "50-69", 70: "70+"}, True
    )
    return df


def assign_ethnicity_white(df: DataFrame, column_name_to_assign: str, ethnicity_group_column_name: str):
    """
    Assign string variable for ethnicity white / non-white based on the 5 major ethnicity groups
    """

    df = df.withColumn(
        column_name_to_assign, F.when(F.col(ethnicity_group_column_name) == "White", "White").otherwise("Non-White")
    )
    return df


def assign_taken_column(df: DataFrame, column_name_to_assign: str, reference_column: str) -> DataFrame:
    """
    Uses references column value to assign a taken column "yes" or "no" depending on whether
    reference is Null
    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), "No").otherwise("Yes"))

    return df


def assign_outward_postcode(df: DataFrame, column_name_to_assign: str, reference_column: str) -> DataFrame:
    """
    Assign column outer postcode with cleaned data from reference postcode column.
    take only left part of postcode and capitalise
    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(column_name_to_assign, F.upper(F.split(reference_column, " ").getItem(0)))
    df = df.withColumn(
        column_name_to_assign, F.when(F.length(column_name_to_assign) > 4, None).otherwise(F.col(column_name_to_assign))
    )

    return df


def assign_column_from_coalesce(df: DataFrame, column_name_to_assign: str, *args) -> DataFrame:
    """
    Assign new column with values from coalesced columns.
    From households_aggregate_processes.xlsx, derivation number 6.
    D6: V1, or V2 if V1 is missing

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    column_name_to_assign: string
    *args: string
        name of columns to coalesce

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    return df.withColumn(colName=column_name_to_assign, col=F.coalesce(*args))


def assign_substring(
    df: DataFrame, column_name_to_assign, column_to_substring, start_position, substring_length
) -> DataFrame:
    """
    Criteria - returns data with new column which is a substring
    of an existing variable
    Parameters
    ----------
    df: pyspark.sql.DataFrame
    new_column_name: string
    column_to_substr: string
    start_position: integer
    len_of_substr: integer

    Return
    ------
    df: pyspark.sql.DataFrame

    """
    df = df.withColumn(column_name_to_assign, F.substring(column_to_substring, start_position, substring_length))

    return df


def assign_school_year(
    df: DataFrame,
    column_name_to_assign: str,
    reference_date_column: str,
    dob_column: str,
    country_column: str,
    school_year_lookup: DataFrame,
) -> DataFrame:
    """
    Assign school year based on date of birth and visit date, accounting for schooling differences by DA.
    From households_aggregate_processes.xlsx, derivation number 31.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    reference_date_column
        Name of column to calculate school year with respect to that point in time
    dob_column
        Name of column specifying date of birth
    country_column
        Name of column specifying country
    school_year_lookup:
        Lookup table defining the school year start day/month and the school year
        reference day/month (which year participant in by dob) by country
    """

    df = (
        df.join(F.broadcast(school_year_lookup), on=country_column, how="left")
        .withColumn(
            "school_start_date",
            F.when(
                (F.month(dob_column) > F.col("school_year_ref_month"))
                | (
                    (F.month(dob_column) == F.col("school_year_ref_month"))
                    & (F.dayofmonth(dob_column) >= F.col("school_year_ref_day"))
                ),
                F.to_date(
                    F.concat(F.year(dob_column) + 5, F.col("school_start_month"), F.col("school_start_day")),
                    format="yyyyMMdd",
                ),
            ).otherwise(
                F.to_date(
                    F.concat(F.year(dob_column) + 4, F.col("school_start_month"), F.col("school_start_day")),
                    format="yyyyMMdd",
                )
            ),
        )
        .withColumn(
            column_name_to_assign,
            F.floor(F.datediff(F.col(reference_date_column), F.col("school_start_date")) / 365.25).cast("integer"),
        )
        # Below statement is to recreate Stata code (school years in DAs don't follow the same pattern),
        #  though need to confirm if this is accurate
        # .withColumn(column_name_to_assign, F.when((F.col(country_column)==F.lit("NI")) /
        # | (F.col(country_column)==F.lit("Scotland")), F.col(column_name_to_assign)+1)
        #                                     .otherwise(F.col(column_name_to_assign)))
        .withColumn(
            column_name_to_assign,
            F.when(
                (F.col(column_name_to_assign) >= F.lit(14)) | (F.col(column_name_to_assign) <= F.lit(0)), None
            ).otherwise(F.col(column_name_to_assign)),
        )
        .drop(
            "school_start_month",
            "school_start_day",
            "school_year_ref_month",
            "school_year_ref_day",
            "school_start_date",
        )
    )

    return df


def derive_cq_pattern(df: DataFrame, column_names, spark_session) -> DataFrame:
    """
    Derive a new column containing string of pattern in
    ["N only", "OR only", "S only", "OR+N", "OR+S", "N+S", "OR+N+S", NULL]
    indicating which ct_* columns indicate a positive result.
    From households_aggregate_processes.xlsx, derivation number 7.

    Parameters
    ----------
    df: pyspark.sql.DataFrame
    column_names: list of string
    spark_session: pyspark.sql.SparkSession

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    assert len(column_names) == 3

    indicator_list = ["indicator_" + column_name for column_name in column_names]

    lookup_df = spark_session.createDataFrame(
        data=[
            (0, 0, 0, None),
            (1, 0, 0, "OR only"),
            (0, 1, 0, "N only"),
            (0, 0, 1, "S only"),
            (1, 1, 0, "OR+N"),
            (1, 0, 1, "OR+S"),
            (0, 1, 1, "N+S"),
            (1, 1, 1, "OR+N+S"),
        ],
        schema=indicator_list + ["cq_pattern"],
    )

    for column_name in column_names:
        df = df.withColumn("indicator_" + column_name, F.when(F.col(column_name) > 0, 1).otherwise(0))

    df = df.join(F.broadcast(lookup_df), on=indicator_list, how="left").drop(*indicator_list)

    return df


def mean_across_columns(df: DataFrame, new_column_name: str, column_names: list) -> DataFrame:
    """
    Create a new column containing the mean of multiple existing columns.

    # Caveat:
    # 0 values are treated as nulls.

    Parameters
    ----------
    df
    new_column_name
        name of column to be created
    column_names
        list of column names to calculate mean across
    """
    columns = [F.col(name) for name in column_names]

    df = df.withColumn("temporary_column_count", F.lit(0))
    for column in column_names:
        df = df.withColumn(
            "temporary_column_count",
            F.when((F.col(column) > 0), F.col("temporary_column_count") + 1).otherwise(F.col("temporary_column_count")),
        )

    # Sum with NULL values removed
    average_expression = sum(F.coalesce(column, F.lit(0)) for column in columns) / F.col("temporary_column_count")
    df = df.withColumn(new_column_name, average_expression)
    df = df.drop("temporary_column_count")
    return df


def assign_date_difference(
    df: DataFrame,
    column_name_to_assign: str,
    start_reference_column: str,
    end_reference_column: str,
    format: Optional[str] = "days",
) -> DataFrame:
    """
    Calculate the difference in days between two dates.
    From households_aggregate_processes.xlsx, derivation number 27.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    start_reference_column
        First date column name.
    end_reference_column
        Second date column name.
    format
        time format (days, weeks, months)
    Return
    ------
    pyspark.sql.DataFrame
    """
    allowed_formats = ["days", "weeks"]
    if format in allowed_formats:
        if start_reference_column == "survey start":
            start = F.to_timestamp(F.lit("2020-05-11 00:00:00"))
        else:
            start = F.col(start_reference_column)
        modifications = {"weeks": F.floor(F.col(column_name_to_assign) / 7)}
        df = df.withColumn(column_name_to_assign, F.datediff(end=F.col(end_reference_column), start=start))
        if format in modifications:
            df = df.withColumn(column_name_to_assign, modifications[format])
        return df.withColumn(column_name_to_assign, F.col(column_name_to_assign).cast("integer"))
    else:
        raise TypeError(f"{format} format not supported")


def assign_column_uniform_value(df: DataFrame, column_name_to_assign: str, uniform_value) -> DataFrame:
    """
    Assign a column with a uniform value.
    From households_aggregate_processes.xlsx, derivation number 11.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    uniform_value
        Value to be set in column.

    Return
    ------
    pyspark.sql.DataFrame


    Notes
    -----
    uniform_value will work as int, float, bool, str, datetime -
            iterables/collections raise errors.
    """
    return df.withColumn(column_name_to_assign, F.lit(uniform_value))


def assign_column_regex_match(
    df: DataFrame, column_name_to_assign: str, reference_column: str, pattern: str
) -> DataFrame:
    """
    Assign a boolean column based on a regex match on reference column.
    From households_aggregate_processes.xlsx, derivation number 12.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column that will be matched
    pattern
        Regular expression pattern as a string
        Needs to be a raw string literal (preceeded by r"")

    Returns
    -------
    pyspark.sql.DataFrame
    """
    return df.withColumn(column_name_to_assign, F.col(reference_column).rlike(pattern))


def assign_consent_code(df: DataFrame, column_name_to_assign: str, reference_columns: list) -> DataFrame:
    """
    Assign new column of value for the maximum consent version.
    From households_aggregate_processes.xlsx, derivation number 19.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_columns list[str]
        Consent columns with 1,0 values used to determine
        consent value.

    Returns
    -------
    pyspark.sql.DataFrame

    Notes
    -----
    Extracts digit value from column name using r'\\d+' pattern.
    """
    assert len(set(reference_columns).difference(set(df.schema.names))) == 0, "Reference columns not in df"

    # assumes only one match in the pattern
    consent_digit_values = [int(re.findall(r"\d+", column)[-1]) for column in reference_columns]

    temp_column_names = [column + "_temp" for column in reference_columns]

    consent_triplets = zip(reference_columns, temp_column_names, consent_digit_values)

    for consent_column, temp_consent_column, consent_value in consent_triplets:
        df = df.withColumn(temp_consent_column, (F.col(consent_column) * F.lit(consent_value)))

    return df.withColumn(column_name_to_assign, F.greatest(*temp_column_names)).drop(*temp_column_names)


def assign_column_to_date_string(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    time_format: str = "yyyy-MM-dd",
    lower_case: bool = False,
) -> DataFrame:
    """
    Assign a column with a TimeStampType to a formatted date string.
    Does not use a DateType object, as this is incompatible with out HIVE tables.
    From households_aggregate_processes.xlsx, derivation number 13.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column of TimeStamp type to be converted
    time_format
        as a string and by using the accepted characters of Pyspark, define what time format is required
        by default, it will be yyyy-MM-dd, e.g. 2021-01-03

    Returns
    -------
    pyspark.sql.DataFrame
    """
    df = df.withColumn(column_name_to_assign, F.date_format(F.col(reference_column), time_format))

    if lower_case:
        df = df.withColumn(column_name_to_assign, F.lower(F.col(column_name_to_assign)))
    return df


def assign_single_column_from_split(
    df: DataFrame, column_name_to_assign: str, reference_column: str, split_on: str = " ", item_number: int = 0
):
    """
    Assign a single column with the values from an item within a reference column that has been split.
    Can specify the split string and item number.

    Gets the first item after splitting on single space (" ") by default.

    Returns null when the specified item does not exist in the split.

    From households_aggregate_processes.xlsx, derivation number 14.
        Column of TimeStamp type to be converted

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column to be
    split_on, optional
        Pattern to split reference_column on
    item_number, optional
        0-indexed number of the item to be selected from the split

    Returns
    -------
    pyspark.sql.DataFrame
    """

    return df.withColumn(column_name_to_assign, F.split(F.col(reference_column), split_on).getItem(item_number))


def assign_isin_list(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    values_list: List[Union[str, int]],
    true_false_values: List[Union[str, int]],
) -> DataFrame:
    """
    Create a new column containing either 1 or 0 derived from values in a list, matched
    with existing values in the database (null values will be carried forward as null)
    From households_aggregate_processes.xlsx, derivation number 10.

    Parameters
    ----------
    df
    column_name_to _assign
        new or existing
    reference_column_name
        name of column to check for list values
    values_list
        list of values to check against reference column
    true_false_values
        true value (index 0), false value (index 1)
    Return
    ------
    pyspark.sql.DataFrame
    """
    return df.withColumn(
        column_name_to_assign,
        F.when((F.col(reference_column).isin(values_list)), true_false_values[0])
        .when((~F.col(reference_column).isin(values_list)), true_false_values[1])
        .otherwise(None),
    )


def assign_from_lookup(
    df: DataFrame, column_name_to_assign: str, reference_columns: list, lookup_df: DataFrame
) -> DataFrame:
    """
    Assign a new column based on values from a lookup DF (null values will be carried forward as null)
    From households_aggregate_processes.xlsx, derivation number 10

    Parameters
    ----------
    pyspark.sql.DataFrame
    column_name_to_assign
    reference_columns
    lookup_df

    Return
    ------
    pyspark.sql.DataFrame
    """

    not_in_df = [reference_column for reference_column in reference_columns if reference_column not in df.columns]

    if not_in_df:
        raise ValueError(f"Columns don't exist in Dataframe: {', '.join(not_in_df)}")

    not_in_lookup = [
        reference_column for reference_column in reference_columns if reference_column not in lookup_df.columns
    ]

    if not_in_lookup:
        raise ValueError(f"Columns don't exist in Lookup: {', '.join(not_in_lookup)}")

    if column_name_to_assign not in lookup_df.columns:
        raise ValueError(f"Column to assign does not exist in lookup: {column_name_to_assign}")

    filled_columns = [
        F.when(F.col(column_name).isNull(), F.lit("_")).otherwise(F.col(column_name))
        for column_name in reference_columns
    ]

    df = df.withColumn("concat_columns", F.concat(*filled_columns))

    lookup_df = lookup_df.withColumn("concat_columns", F.concat(*filled_columns))

    lookup_df = lookup_df.drop(*reference_columns)

    return df.join(F.broadcast(lookup_df), df.concat_columns.eqNullSafe(lookup_df.concat_columns), how="left").drop(
        "concat_columns"
    )


def assign_age_at_date(df: DataFrame, column_name_to_assign: str, base_date, date_of_birth) -> DataFrame:
    """
    Assign a new column containing age at a specified date
    Assume that parameters will be in date format
    The function will not correctly account for leap years

    Parameters
    ----------
    pyspark.sql.DataFrame
    base_date
    date_of_birth

    Return
    ------
    pyspark.sql.DataFrame
    """

    df = df.withColumn("date_diff", F.datediff(base_date, date_of_birth)).withColumn(
        column_name_to_assign, F.floor(F.col("date_diff") / 365.25).cast("integer")
    )

    return df.drop("date_diff")


def assign_correct_age_at_date(df: DataFrame, column_name_to_assign, reference_date, date_of_birth) -> DataFrame:
    """
    Uses correct logic to calculate complete years elapsed between 2 dates
    """
    df = df.withColumn(
        "month_more",
        F.when(F.month(F.col(reference_date)) > F.month(F.col(date_of_birth)), 2).otherwise(
            F.when(F.month(F.col(reference_date)) == F.month(F.col(date_of_birth)), 1).otherwise(0)
        ),
    )
    df = df.withColumn(
        "day_more",
        F.when(F.date_format(F.col(reference_date), "d") >= F.date_format(F.col(date_of_birth), "d"), 1).otherwise(0),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.year(F.col(reference_date))
        - F.year(F.col(date_of_birth))
        - F.lit(1)
        + F.round((F.col("month_more") + F.col("day_more")) / 3, 0).cast("int"),
    )
    return df.drop("month_more", "day_more")


def assign_grouped_variable_from_days_since(
    df: DataFrame,
    binary_reference_column: str,
    days_since_reference_column: str,
    column_name_to_assign: str,
) -> DataFrame:
    """
    Function create variables applied for days_since_think_had_covid_group and
    contact_known_or_suspected_covid_days_since_group. The variable
    days_since_think_had_covid and contact_known_or_suspected_covid_days_since will
    give a number that will be grouped in a range.
    Parameters
    ----------
    df
    binary_reference_column
        yes/no values that describe whether the patient thinks have had covid
    days_since_reference_column
        column from which extract the number of days transcurred that needs to
        be grouped
    column_name_to_assign
        grouping column
    """
    df = assign_named_buckets(
        df=df,
        reference_column=days_since_reference_column,
        column_name_to_assign=column_name_to_assign,
        map={0: "0-14", 15: "15-28", 29: "29-60", 61: "61-90", 91: "91+"},
    )
    return df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(binary_reference_column) == "Yes") & (F.col(days_since_reference_column).isNull()), "Date not given"
        )
        .otherwise(F.col(column_name_to_assign))
        .cast("string"),
    )


def assign_raw_copies(df: DataFrame, reference_columns: list) -> DataFrame:
    """Create a copy of each column in a list, with a '_raw' suffix."""
    for column in reference_columns:
        df = df.withColumn(column + "_raw", F.col(column).cast(df.schema[column].dataType))
    return df


def assign_work_health_care(
    df, column_name_to_assign, direct_contact_column, reference_health_care_column, other_health_care_column
) -> DataFrame:
    """
    Combine the different versions of work health care responses.
    Uses direct contact status to edit these.

    Parameters
    ----------
    df
    column_name_to_assign
    direct_contact_column
        Column indicating direct contact as Yes/No
    reference_health_care_column
        Column to coalesce with, having desired answer format
    other_health_care_column
        Column to be edited to match the reference answer format
    """
    health_care_map = {
        "Yes, in primary care, e.g. GP, dentist": "Yes, primary care",
        "Yes, in secondary care, e.g. hospital": "Yes, secondary care",
        "Yes, in other healthcare settings, e.g. mental health": "Yes, other healthcare",
    }
    value_map = F.create_map([F.lit(x) for x in chain(*health_care_map.items())])
    patient_facing_text = F.when(F.col(direct_contact_column) == "Yes", ", patient-facing").otherwise(
        ", non-patient-facing"
    )
    edited_other_health_care_column = F.when(
        (F.col(other_health_care_column) != "No") & F.col(other_health_care_column).isNotNull(),
        F.concat(value_map[F.col(other_health_care_column)], patient_facing_text),
    ).otherwise(F.col(other_health_care_column))
    df = df.withColumn(
        column_name_to_assign, F.coalesce(F.col(reference_health_care_column), edited_other_health_care_column)
    )
    return df


def contact_known_or_suspected_covid_type(
    df: DataFrame,
    contact_known_covid_type_column: str,
    contact_any_covid_type_column: str,
    contact_any_covid_date_column: str,
    contact_known_covid_date_column: str,
    contact_suspect_covid_date_column: str,
):
    """
    Parameters
    ----------
    df
    contact_known_covid_type_column
    contact_suspect_covid_type_column
    contact_any_covid_date_column
    contact_known_covid_date_column
    contact_suspect_covid_date_column
    """
    df = df.withColumn(
        contact_any_covid_type_column,
        F.when(
            F.col(contact_any_covid_date_column) == F.col(contact_known_covid_date_column),
            F.col(contact_known_covid_type_column),
        )
        .when(
            F.col(contact_any_covid_date_column) == F.col(contact_suspect_covid_date_column),
            F.col(contact_known_covid_type_column),
        )
        .otherwise(None),
    )
    return df


def derive_household_been_columns(
    df: DataFrame,
    column_name_to_assign: str,
    individual_response_column: str,
    household_response_column: str,
) -> DataFrame:
    """
    Combines a household and individual level response, to an overall household response.
    Assumes input responses are 'Yes'/'no'.
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col(individual_response_column) == "Yes"), "Yes, I have")
        .when(
            ((F.col(individual_response_column) != "Yes") | F.col(individual_response_column).isNull())
            & (F.col(household_response_column) == "Yes"),
            "No I haven’t, but someone else in my household has",
        )
        .otherwise("No, no one in my household has"),
    )
    return df


def aggregated_output_groupby(
    df: DataFrame,
    column_group: str,
    apply_function_list: List[str],
    column_name_list: List[str],
    column_name_to_assign_list: List[str],
) -> DataFrame:
    """
    Parameters
    ----------
    df
    column_group
    apply_function_list
    column_apply_list
    column_name_to_assign_list
    """
    function_object_list = [
        getattr(F, function)(col_name) for col_name, function in zip(column_name_list, apply_function_list)
    ]
    return df.groupBy(column_group).agg(
        *[
            apply_function.alias(column_name_to_assign)
            for apply_function, column_name_to_assign in zip(function_object_list, column_name_to_assign_list)
        ]
    )


def aggregated_output_window(
    df: DataFrame,
    column_window_list: List[str],
    column_name_list: List[str],
    apply_function_list: List,
    column_name_to_assign_list: List[str],
    order_column_list: List[str] = [],
) -> DataFrame:
    """
    Parameters
    ----------
    df
    column_group_list
    apply_function_list
    column_apply_list
    column_name_to_assign_list
    order_column_list
    when_condition
    """
    window = Window.partitionBy(*column_window_list).orderBy(*order_column_list)
    function_object_list = [
        getattr(F, function)(col_name) for col_name, function in zip(column_name_list, apply_function_list)
    ]
    for apply_function, column_name_to_assign in zip(function_object_list, column_name_to_assign_list):
        df = df.withColumn(column_name_to_assign, apply_function.over(window))
    return df
