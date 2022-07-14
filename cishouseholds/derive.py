import re
from functools import reduce
from itertools import chain
from operator import add
from operator import and_
from operator import or_
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.expressions import all_equal
from cishouseholds.expressions import all_equal_or_Null
from cishouseholds.expressions import any_column_matches_regex
from cishouseholds.pyspark_utils import get_or_create_spark_session


def assign_datetime_from_coalesced_columns_and_log_source(
    df: DataFrame,
    column_name_to_assign: str,
    primary_datetime_columns: List[str],
    secondary_date_columns: List[str],
    file_date_column: str,
    min_date: str,
    source_reference_column_name: str,
    default_timestamp: str,
):

    """
    Assign a timestamp column from coalesced list of columns with a default timestamp if timestamp missing in column
    """
    coalesce_columns = [
        F.col(datetime_column) for datetime_column in [*primary_datetime_columns, *secondary_date_columns]
    ]
    coalesce_columns = [F.when(col.between(F.lit(min_date), F.col(file_date_column)), col) for col in coalesce_columns]

    column_names = primary_datetime_columns + secondary_date_columns
    source_columns = [
        F.when(column_object.isNotNull(), column_name)
        for column_object, column_name in zip(coalesce_columns, column_names)
    ]
    df = df.withColumn(source_reference_column_name, F.coalesce(*source_columns))
    df = df.withColumn(
        column_name_to_assign,
        F.to_timestamp(
            F.when(
                F.col(source_reference_column_name).isin(secondary_date_columns),
                F.concat_ws(" ", F.date_format(F.coalesce(*coalesce_columns), "yyyy-MM-dd"), F.lit(default_timestamp)),
            ).otherwise(F.coalesce(*coalesce_columns)),
            format="yyyy-MM-dd HH:mm:ss",
        ),
    )
    return df


def assign_date_from_filename(df: DataFrame, column_name_to_assign: str, filename_column: str):
    """
    Populate a pyspark date column with the date contained in the filename column
    """
    date = F.regexp_extract(F.col(filename_column), r"_(\d{8})(_\d{6})?[.](csv|txt)", 1)
    time = F.when(
        F.regexp_extract(F.col(filename_column), r"_(\d{8})(_\d{6})?[.](csv|txt)", 2) == "", "_000000"
    ).otherwise(F.regexp_extract(F.col(filename_column), r"_(\d{8})(_\d{6})?[.](csv|txt)", 2))
    df = df.withColumn(column_name_to_assign, F.to_timestamp(F.concat(date, time), format="yyyyMMdd_HHmmss"))
    return df


def assign_visit_order(df: DataFrame, column_name_to_assign: str, visit_date_column: str, id_column: str):
    """
    assign an incremental count to each participants visit

    Parameters
    -------------
    column_name_to_assign
        column_name_to_assign: column to show count
    visit_date_column
        visit_date_column: date column to base count on
    id_column
        id_column: The column where the window (subset) is based, then the count occurs
    """
    window = Window.partitionBy(id_column).orderBy(visit_date_column)
    df = df.withColumn(column_name_to_assign, F.row_number().over(window))
    return df


def map_options_to_bool_columns(df: DataFrame, reference_column: str, value_column_name_map: dict, sep: str):
    """
    map column containing multiple value options to new columns containing true/false based on if their
    value is chosen as the option.
    Parameters
    df
    reference_column
        column containing option values
    value_column_name_map
        mapping expression of column names to assign and options within reference column
    """
    df = df.withColumn(reference_column, F.split(F.col(reference_column), sep))
    for val, col in value_column_name_map.items():
        df = df.withColumn(col, F.when(F.array_contains(reference_column, val), "Yes"))
    return df.withColumn(reference_column, F.array_join(reference_column, sep))


def assign_column_value_from_multiple_column_map(
    df: DataFrame, column_name_to_assign: str, value_to_condition_map: List[List[Any]], column_names: List[str]
):
    """
    assign column value based on values of any number of columns in a dictionary
    Parameters
    ----------
    column_name_to_assign
    value_to_condition_map
        a list of column value options to map to each resultant value in the 'column_name_to_assign'.
        multiple sublists are optional within this input and denote the option to have multiple optional values.
    column_names
        a list of column names in the same order as the values expressed in the 'value_to_condition_map' input
    Example
    -------
    A | B
    1 | 0
    1 | 1
    0 | 0
    value_to_condition_map = [
        [Yes,[1,0]],
        [No,[1,1]]
    ]
    column_names = [A,B]
    ~ with a value of 1 and 0 in columns A and B respectively the result column C would be set to Yes and with
    1 and 1 in the same columns the result would b No and an unmapped result yields None. ~
    A | B | C
    1 | 0 | Yes
    1 | 1 | No
    0 | 0 |
    """
    df = df.withColumn(column_name_to_assign, F.lit(None))
    for row in value_to_condition_map:
        mapped_value = row[0]
        values = row[1]
        logic = []
        for col, val in zip(column_names, values):
            if type(val) == list:
                logic.append(reduce(or_, [F.col(col).eqNullSafe(option) for option in val]))
            else:
                logic.append(F.col(col).eqNullSafe(val))
        df = df.withColumn(
            column_name_to_assign,
            F.when(reduce(and_, logic), mapped_value).otherwise(F.col(column_name_to_assign)),
        )
    return df


def concat_fields_if_true(
    df: DataFrame, column_name_to_assign: str, column_name_pattern: str, true_value: str, sep: str = ""
):
    """
    concat the names of fields where a given condition is met to form a new column
    """
    columns = [col for col in df.columns if re.match(column_name_pattern, col)]
    df = df.withColumn(
        column_name_to_assign,
        F.concat_ws(sep, *[F.when(F.col(col) == true_value, col).otherwise(None) for col in columns]),
    )
    return df


def derive_had_symptom_last_7days_from_digital(
    df: DataFrame, column_name_to_assign: str, symptom_column_prefix: str, symptoms: List[str]
):
    """
    Derive symptoms in v2 format from digital file
    """
    symptom_columns = [f"{symptom_column_prefix}{symptom}" for symptom in symptoms]

    df = count_value_occurrences_in_column_subset_row_wise(df, "NUM_NO", symptom_columns, "No")
    df = count_value_occurrences_in_column_subset_row_wise(df, "NUM_YES", symptom_columns, "Yes")
    df = df.withColumn(
        column_name_to_assign,
        F.when(F.col("NUM_YES") > 0, "Yes").when(F.col("NUM_NO") > 0, "No").otherwise(None),
    )
    return df.drop("NUM_YES", "NUM_NO")


def assign_visits_in_day(df: DataFrame, column_name_to_assign: str, visit_date_column: str, participant_id_column: str):
    """
    Count number of visits of each participant in a given day
    Parameters
    ----------
    """
    window = Window.partitionBy(participant_id_column, F.to_date(visit_date_column))
    df = df.withColumn(column_name_to_assign, F.sum(F.lit(1)).over(window))
    return df


def count_barcode_cleaned(
    df: DataFrame, column_name_to_assign: str, barcode_column: str, date_taken_column: str, visit_datetime_colum: str
):
    """
    Count occurrences of barcode
    Parameters
    ----------
    df
    column_name_to_assign
    barcode_column
    """
    window = Window.partitionBy(barcode_column)
    df = df.withColumn(
        column_name_to_assign,
        F.sum(
            F.when(
                (F.col(barcode_column).isNotNull())
                & (F.col(barcode_column) != "ONS00000000")
                & (F.datediff(F.col(visit_datetime_colum), F.col(date_taken_column)) <= 14),
                1,
            ).otherwise(0)
        ).over(window),
    )
    return df


def assign_fake_id(df: DataFrame, column_name_to_assign: str, reference_column: str):
    """
    Derive an incremental id from a reference column containing an id
    """
    df_unique_id = df.select(reference_column).distinct()
    df_unique_id = df_unique_id.withColumn("TEMP", F.lit(1))
    window = Window.partitionBy(F.col("TEMP")).orderBy(reference_column)
    df_unique_id = df_unique_id.withColumn(column_name_to_assign, F.row_number().over(window))  # or dense_rank()

    df = df.join(df_unique_id, on=reference_column, how="left")
    return df.drop("TEMP")


def assign_distinct_count_in_group(
    df, column_name_to_assign: str, count_distinct_columns: List[str], group_by_columns: List[str]
):
    """
    Window-based count of distinct values by group

    Parameters
    ----------
    count_distinct_columns
        columns to determine distinct records
    group_by_columns
        columns to group by and count within
    """
    count_distinct_columns_window = Window.partitionBy(*count_distinct_columns).orderBy(F.lit(0))
    group_window = Window.partitionBy(*group_by_columns)
    df = df.withColumn(
        column_name_to_assign,
        F.sum(F.when(F.row_number().over(count_distinct_columns_window) == 1, 1)).over(group_window).cast("integer"),
    )
    return df


def assign_count_by_group(df: DataFrame, column_name_to_assign: str, group_by_columns: List[str]):
    """
    Window-based count of all rows by group

    Parameters
    ----------
    group_by_columns
        columns to group by and count within
    """
    count_window = Window.partitionBy(*group_by_columns)
    df = df.withColumn(column_name_to_assign, F.count("*").over(count_window).cast("integer"))
    return df


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


def assign_household_under_2_count(
    df: DataFrame, column_name_to_assign: str, column_pattern: str, condition_column: str
):
    """
    Count number of individuals below two from age (months) columns matching pattern.
    if condition column is 'No' it will only count so long the range of columns ONLY have only 0s or nulls.
    Parameters
    ----------
    df
    column_name_to_assign
    column_pattern
    condition_column
    """
    columns_to_count = [column for column in df.columns if re.match(column_pattern, column)]
    count = reduce(
        add, [F.when((F.col(column) >= 0) & (F.col(column) <= 24), 1).otherwise(0) for column in columns_to_count]
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            ((F.col(condition_column) == "Yes") & (~all_equal(columns_to_count, 0)))
            | ((F.col(condition_column) == "No") & (~all_equal_or_Null(columns_to_count, 0))),
            count,
        ).otherwise(0),
    )
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
    df: DataFrame, column_name_to_assign: str, selection_columns: List[str], count_if_value: Union[str, int, None]
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
    df = df.withColumn(column_name_to_assign, F.array([F.col(col) for col in selection_columns]))
    if count_if_value is None:
        df = df.withColumn(column_name_to_assign, F.expr(f"filter({column_name_to_assign}, x -> x is not null)"))
    else:
        df = df.withColumn(column_name_to_assign, F.array_remove(column_name_to_assign, count_if_value))
    df = df.withColumn(column_name_to_assign, F.lit(len(selection_columns) - F.size(F.col(column_name_to_assign))))
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
            F.col(work_sector_column) != "Social care",
            "No",
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
        ),
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
        column_name_to_assign,
        F.when(F.col(ethnicity_group_column_name) == "White", "White")
        .when(F.col(ethnicity_group_column_name) != "White", "Non-White")
        .otherwise(None)
        .cast("string"),
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
    df = df.withColumn(column_name_to_assign, F.rtrim(F.regexp_replace(F.col(reference_column), r".{3}$", "")))
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
        Needs to be a raw string literal (preceded by r"")

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
    Assign a column with a TimeStampType to a formatted date string.gg
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


def assign_work_health_care(df, column_name_to_assign, direct_contact_column, health_care_column) -> DataFrame:
    """
    Combine direct contact and health care responses to get old format of health care responses.

    Parameters
    ----------
    df
    column_name_to_assign
    direct_contact_column
        Column indicating whether participant works in direct contact
    health_care_column
        Column indicating if participant works in health care
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
        (F.col(health_care_column) != "No") & F.col(health_care_column).isNotNull(),
        F.concat(value_map[F.col(health_care_column)], patient_facing_text),
    ).otherwise(F.col(health_care_column))
    df = df.withColumn(column_name_to_assign, edited_other_health_care_column)
    return df


def assign_work_status_group(df: DataFrame, colum_name_to_assign: str, reference_column: str):
    """
    Assigns a string group based on work status. Uses minimal work status categories (voyager 0).
    Results in groups of:
    - Unknown (null)
    - Student
    - Employed
    - Not working (unemployed, retired, long term sick etc)
    """
    df = df.withColumn(
        colum_name_to_assign,
        F.when(
            F.col(reference_column).isin(["Employed", "Self-employed", "Furloughed (temporarily not working)"]),
            "Employed",
        )
        .when(F.col(reference_column).isNull(), "Unknown")
        .otherwise(F.col(reference_column)),
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


def regex_match_result(
    columns_to_check_in: List[str],
    positive_regex_pattern: str,
    negative_regex_pattern: Optional[str] = None,
    debug_mode: bool = False,
) -> Union[F.Column, Tuple[F.Column, F.Column, F.Column]]:
    """
    A generic function which applies the user provided RegEx patterns to a list of columns. If a value in any
    of the columns matches the `positive_regex_pattern` pattern but not the `negative_regex_pattern` pattern
    then the result of the match will be set to (bool) True, False otherwise.

    The Truth Table below shows how the final pattern matching result is arrived at.

    +----------------------+----------------------+-----+
    |positive_regex_pattern|negative_regex_pattern|final|
    +----------------------+----------------------+-----+
    |                  true|                  true|false|
    |                  true|                 false| true|
    |                 false|                  true|false|
    |                 false|                 false|false|
    +----------------------+----------------------+-----+

    Parameters:
    -----------
    columns_to_check_in
        a list of columns in which to look for the `positive_regex_pattern`
    positive_regex_pattern
        the Spark-compatible regex pattern match against
    negative_regex_pattern
        (optional) the Spark-compatible regex pattern to NOT match against.
    debug_mode:
        If True and `negative_regex_pattern` is not None, then result of applying positive_regex_pattern and
        negative_regex_pattern are turned in addition to the final result.

    Returns:
    --------
    Column
        The final result of applying positive_regex_pattern and negative_regex_pattern (if given)
    Tuple[Column, Column, Column]
        Returned when `debug_mode` is True and `negative_regex_pattern` is not None. First element
        is the result of applying `positive_regex_pattern`, second element is the result of applying
        `negative_regex_pattern` and 3rd is the final result of applying positive_regex_pattern and
        negative_regex_pattern
    """
    positive_regex_match_result = any_column_matches_regex(columns_to_check_in, positive_regex_pattern)

    if negative_regex_pattern is None:
        result = positive_regex_match_result
    else:
        negative_regex_match_result = any_column_matches_regex(columns_to_check_in, negative_regex_pattern)
        result = positive_regex_match_result & ~negative_regex_match_result

        if debug_mode:
            result = positive_regex_match_result, negative_regex_match_result, result

    return result


def assign_regex_match_result(
    df: DataFrame,
    columns_to_check_in: List[str],
    positive_regex_pattern: str,
    column_name_to_assign: str,
    negative_regex_pattern: Optional[str] = None,
    debug_mode: bool = False,
) -> DataFrame:
    """
    A generic function which applies the user provided RegEx patterns to a list of columns. If a value in any
    of the columns matches the `positive_regex_pattern` pattern but not the `negative_regex_pattern` pattern
    then the result of the match will be set to (bool) True, False otherwise.

    The Truth Table below shows how the final pattern matching result is assigned.

    +----------------------+----------------------+-----+
    |positive_regex_pattern|negative_regex_pattern|final|
    +----------------------+----------------------+-----+
    |                  true|                  true|false|
    |                  true|                 false| true|
    |                 false|                  true|false|
    |                 false|                 false|false|
    +----------------------+----------------------+-----+

    Parameters:
    -----------
    df
        The input dataframe to process
    columns_to_check_in
        a list of columns in which to look for the `positive_regex_pattern`
    positive_regex_pattern
        the Spark-compatible regex pattern match against
    negative_regex_pattern
        (optional) the Spark-compatible regex pattern to NOT match against. If given and `column_name_to_assign` is not
        None, then two additional columns of the form: f"{column_name_to_assign}_positive" &
        f"{column_name_to_assign}_negative" are created which track the matches against the positive and negative regex
        patterns respectively. Set `debug_mode` to True to expose these columns in the returned dataframe.
    column_name_to_assign
        (optional) if this is none, then we return a PySpark Column object containing the result of the RegEx pattern
        search, otherwise we return a DataFrame with `column_name_to_assign` as the column containing the result.
    debug_mode:
        Only relevant when `column_name_to_assign` is not None - See `negative_regex_pattern` above.

    See Also:
    ---------
    regex_match_result: `assign_regex_match_result` wraps around `regex_match_result`
    """
    match_result = regex_match_result(
        columns_to_check_in=columns_to_check_in,
        positive_regex_pattern=positive_regex_pattern,
        negative_regex_pattern=negative_regex_pattern,
        debug_mode=debug_mode,
    )

    if type(match_result) == tuple:
        # a tuple is returned when debug_mode is True
        positive_regex_match_result, negative_regex_match_result, result = match_result

        df = (
            df.withColumn(f"{column_name_to_assign}_positive", positive_regex_match_result)
            .withColumn(
                f"{column_name_to_assign}_negative",
                negative_regex_match_result,
            )
            .withColumn(column_name_to_assign, result)
        )
    else:
        df = df.withColumn(column_name_to_assign, match_result)

    return df


def derive_patient_facing_variables(
    job_main_resp1: str,
    work_status: str,
    patient_facing_orig: str,
    work_direct_contact_patients_etc: str,
    job_title1,  # TODO: job_title1 and main_resp1 are derived, remove them
    main_resp1,
) -> DataFrame:
    """
    Parameters
    ----------
    df
    job_main_resp1
    work_status
    patient_facing_orig
    work_direct_contact_patients_etc

    """
    flag_vet = F.col(job_main_resp1).rlike(
        r"\bVETS*\b|\bVEN?T[A-Z]*(RY|IAN)\b|EQUIN|\b(DOG|CAT)\b|HEDGEHOG|ANIMAL"  # noqa: E501
    ) & ~F.col(job_main_resp1).rlike(r"VET PEOPLE")

    # Admin
    flag_admin = F.col(job_main_resp1, "\bADMIN(?!IST[EO]R)|ADM[A-Z]{2,}RAT[EO]R|CLERICAL|CLERK")
    flag_hc_admin = (
        (flag_admin & F.col(job_main_resp1).rlike(r"NHS|HOSPITAL|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE"))
        | F.col(job_main_resp1).rlike(r"CLINICAL *CODER|\bWARD *CLERK")
    ) & ~F.col(job_main_resp1).rlike(r"STATIST")
    flag_nhc_admin = (
        flag_admin
        & F.col(job_main_resp1).rlike(r"SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|^BANK CLERK|\bCHURCH\b")
        & (F.col(flag_hc_admin) is False)
    )

    # Counsellor
    # flag_counsellor = F.col(job_main_resp1).rlike(r"COUNS|COUNC") # TODO: investigate why never used
    flag_nhc_counsellor = F.col(job_main_resp1).rlike(r"COUNS|COUNC") & (
        F.col(job_main_resp1).rlike(
            r"REPRESENT|BUSINESS|POLI(C|T)|CAREER|DISTRICT|LOCAL|COUNTY|DEBT|CITY|COUNCIL\s|COUNCIL$|ACCOUNTANT|SOLICITOR|LAW|CHAPLAN|CHAPLAIN|DEFENCE|GOVERNMENT|PARISH|LAWYER|ASSESSOR|CURRICULUM|LEGAL|PRISONER|FARMER"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(r"CASEWORK|CARE WORK|SCHOOL|LECTURER|COLLEGE|TEACH")
    )
    flag_hc_counsellor = F.col(job_main_resp1).rlike(r"COUNS|COUNC") & F.col(job_main_resp1).rlike(
        r"ADDICT|VICTIM|TRAUMA|\sMENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE"
    )
    # Receptionist
    flag_receptionist = (
        F.col(job_main_resp1).rlike(r"RECEPTIONIST|OPTICAL ASSISTANT|RECEPTION *(WORK|DUTIES)")
        | (F.col(job_main_resp1).rlike("\bADMIN(?!IST[EO]R)") & F.col(job_main_resp1).rlike("RECEPTION"))
    ) & ~F.col(job_main_resp1).rlike("\bTEACH")
    flag_hc_receptionist = flag_receptionist & F.col(job_main_resp1).rlike(
        r"NHS|HOSPITAL$|OSTEOPATH|OUTPATIENT|HOSPITAL(?!ITY)|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|DENTAL|DENTIST|\bGP\b|\bDOCTOR|OPTICIAN|OPTICAL|CHIROPRAC|A&E"  # noqa: E501
    )
    flag_nhc_receptionist = (
        (
            flag_receptionist
            & (
                F.col(job_main_resp1).rlike(
                    r"SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL|\bCARE *HOME|\bVET[A-Z]*RY\b|HAIR *(SALON|DRESS)+|EDUCATION|SPORT[S ]*CENT|LEISURE|BEAUTY|COLLEGE"  # noqa: E501
                )
                | F.col(job_main_resp1).rlike(r"\bSPA\b|RETAIL|\b\LAW\b\|\bLEGAL|\bBAR WORK|GARAGE|\bVET[S]*\b")
            )
        )
        | (
            F.col(job_main_resp1).rlike(r"RECEPTION")
            & F.col(job_main_resp1).rlike(r"LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL")
        )
    ) & ~flag_hc_receptionist

    # Secretary
    flag_secretary = F.col(job_main_resp1).rlike(r"S.?C+R+.?T+.?R+Y|\sPA\s|P.?RS+.?N+.?L AS+IS+T+AN+")
    flag_hc_secretary = flag_secretary & F.col(job_main_resp1).rlike(
        r"MEDIC.*|HEALTH|NHS|HOSPITAL\s|HOSPITAL$|CLINIC PATIENT|CAMHS|X.?RAY|\sDR\s|DOCTOR|PAEDIATRIC|A&E|DENTIST|PATIENT|GP|DENTAL|SURGERY|OPTI.*|OPTICAL|OPTICIANS"  # noqa: E501
    )
    flag_nhc_secretary = flag_secretary & (
        F.col(job_main_resp1).rlike(
            r"LEGAL|LAW|SOLICITOR|PRODUCTION|PARISH|DOG|INTERNATIONAL|COMPANY|COMPANY|EDUCATION|UNIVERSITY|SCHOOL|TEACHER|FINANCE|BUILDER|BUSINESS|BANK|PROJECT|CHURCH|ESTATE AGENT|MANUFACT|SALE|SPORT|FARM|CLUB"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(r"CONTRACTOR|CIVIL SERV.*|CLERICAL|COUNCIL|MEDICAL SCHOOL|ACCOUNT|CARER|CHARITY")
    )

    # Support Worker
    flag_support = F.col(job_main_resp1).rlike(r"SUP+ORT *WORKER") & ~F.col(job_main_resp1).rlike(r"BUISNESS SUPPORT")
    flag_hc_support = (
        flag_support
        & F.col(job_main_resp1).rlike(
            r"HOSPITAL|HEALTH *CARE|MENTAL *HEALTH|MATERNITY|CLINICAL|WARD|NURSE|NURSING(?! *HOME)|SURGERY|A&E|ONCOLOGY|PHLEBOTOM|AMBULANCE|WARD|MIDWIFE|ACCIDENT *(& *|AND *)*EMERGENCY|COVID.*SWA[BP]"  # noqa: E501
        )
        & ~F.col(job_main_resp1).rlike(
            r"LOCAL COUNCIL|DISCHARGE|POST HOSPITAL|HOME NURS"
        )  # needed to exclude those who deal with discharged hospital patients
    )
    flag_nhc_support = flag_support & ~flag_hc_support & ~F.col(job_main_resp1).rlike(r"HEALTH *CARE ASSIST|\bHCA\b")

    # Care
    flag_house_care = F.col(job_main_resp1).rlike(
        r"(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER"
    ) & ~F.col(job_main_resp1).rlike(
        r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL"
    )
    flag_child_care = F.col(job_main_resp1).rlike(r"CHILD *(CARE|MIND)|NANN[YIE]+\b|AU PAIR") & ~F.col(
        job_main_resp1
    ).rlike(r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL")
    flag_informal_care = (
        F.col(job_main_resp1).rlike(
            r"(((CAR(ER|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM|T*H*E* ELDERLY)"  # noqa: E501
        )
        & ~F.col(job_main_resp1).rlike(r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL")
        & ~(flag_child_care | flag_house_care)
    )
    flag_formal_care = (
        F.col(job_main_resp1).rlike(
            r"^CAE?RE*R *(CARE*|NA)*$|(CARE|NURSING) *HOME|(SOCIAL|COMMUNITY|DOMICIL[IA]*RY)* *CARE|CARE *(WORK|ASSISTANT)|ASST CARING|CARE SUPPORT WORK|SUPPORT *WORKER *CARE|INDEPEND[EA]NT LIVING"  # noqa: E501
        )
        & ~F.col(job_main_resp1).rlike(r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL")
        & ~(flag_informal_care | flag_child_care | flag_house_care)
    )

    # Pharmacy
    flag_pharmacist = F.col(job_main_resp1).rlike(r"PHARMA(?![CS][EU]*TIC)")
    flag_nhc_pharmacist = flag_pharmacist & ~F.col(job_main_resp1).rlike(
        r"ANALYST|CARE HOME|ELECTRICAL|COMPAN(Y|IES)|INDUSTR|DIRECTOR|RESEARCH|WAREHOUSE|LAB|PROJECT|PRODUCTION|PROCESS|LAB|QA|QUALITY"  # noqa: E501
    )
    flag_hc_pharmacist = flag_pharmacist & F.col(job_main_resp1).rlike(
        r"AS+IST|TECHN|RETAIL|DISPEN[SC]|SALES AS+IST|HOSPITAL|PRIMARY CARE|SERVE CUSTOM"  # noqa: E501
    )
    flag_dietician = F.col(job_main_resp1).rlike(
        r"\bD[EIA]{0,2}[TC][EI]?[CT]+[AEIOU]*[NC(RY)]|\bDIET(RIST)?\b"
    ) & ~F.col(job_main_resp1).rlike(r"DETECTION")
    flag_doctor = F.col(job_main_resp1).rlike(
        r"DOCT[EO]R|\bGP\b|GENERAL PRACTI[CIAN|TION]|\bDR\b|CARDIAC|\A ?(&|AND) ?E\|PHYSI[CT]I[AO]"
    ) & ~F.col(job_main_resp1).rlike(r"LECTURER|DOCTORI*AL|RESEARCH|PHD|STAT|WAR|ANIMAL|SALES*|FINANCE")
    flag_dentist = F.col(job_main_resp1).rlike(r"DENTIS.*|\bDENTAL")
    flag_midwife = F.col(job_main_resp1).rlike(r"MI*D*.?WI*F.?E.?|MIDWIV|MID*WIF|HEALTH VISITOR")
    flag_nurse = F.col(job_main_resp1).rlike(r"N[IU]RS[EY]|MATRON|\bHCA\b") & ~F.col(job_main_resp1).rlike(
        r"N[UI][RS]S[EA]R*[YIEU]+|(CARE|NURSING) *HOME|SCHOOL|TEACHER"
    )
    flag_paramedic = F.col(job_main_resp1).rlike(r"PARA *MEDIC|AMBUL[AE]NCE") & ~F.col(job_main_resp1).rlike(r"LECTUR")
    flag_additional_hc = (
        F.col(job_main_resp1).rlike(
            r"SONOGRAPHER|RADIO(GRAPHER|LOGIST)|VAC+INAT[OE]R|(ORTHO(PAEDIC)?|\bENT CONSULTANT|ORAL|EYE)+ SURGEON|SURGEON SURGERY|(DIABETIC EYE|RETINAL) SCRE+NER|(PH|F)LEBOTOM|CLINICAL SCIEN"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(
            r"MEDICAL PHYSICIST|CARDIAC PHYSIOLOG|OSTEOPATH|OPTOMOTRIST|PODIATRIST|OBSTETRI|GYNACOLOG|ORTHO[DOENT]+|OPTI[TC]I[AO]N|CRITICAL CARE PRACTITIONER|HOSPITAL PORTER|AN[AE]STHET[IST|IC|IA]"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(r"PALLIATIVE|DISTRICT NURS|PAEDIATRI[CT]I[AO]N|HAEMATOLOGIST")
    ) & ~F.col(job_main_resp1).rlike(r"LAB MANAGER")

    flag_covid_test = (
        F.col(job_main_resp1).rlike(r"COVID")
        & F.col(job_main_resp1).rlike(r"TEST|SWAB|VAC+INAT|IM+UNIS|SCREEN|WARD")
        & ~F.col(job_main_resp1).rlike(r"LAB|AN[AY]LIST|SCHOOL|MANAGER")
    )
    flag_physiotherapist = F.col(job_main_resp1).rlike(
        r"PH[YI]+SIO|PH[YSIH]+IO\s*THERAPIST|PH[YI]S[IY]CAL\s*REHAB|PH[YI]S[IY]CAL\s*THERAPY"
    ) & ~F.col(job_main_resp1).rlike(r"PHYSIOLOG|PHYSIOSIST")
    flag_nhc_psychologist = F.col(job_main_resp1).rlike(
        r"PHSYCOLOGY|PHYCOLOGIST|PYCHLOLOGIST|PHYCOLOGIST|PSYCHOLOGOLOGIST|PHYCOLIGIST|PYSCOLOGIST|PHYSCHOLOGICAL|PSYCHOLOGICIST|PSYCHOLGIST|PSYCHOLOGIST|PSYCHOLOGY|PSYCHOLOGICAL"  # noqa: E501
    ) & (
        F.col(job_main_resp1).rlike(
            r"EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACHING|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(r"PRISON|OCCUPATION|OCCUPATIONAL|FORENSICS|\bUCL\b")
    )
    flag_social_worker = F.col(job_main_resp1).rlike(r"SOCIAL.*WORK|FOSTER CARE")
    flag_call_handler = (
        F.col(job_main_resp1).rlike(r"111|119|999|911|NHS|TRIAGE|EMERGENCY")
        & F.col(job_main_resp1).rlike(
            r"ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE|TELE(PHONE)?|COVID"
        )
        & ~F.col(job_main_resp1).rlike(r"CUSTOMER SERVICE|SALES")
    )
    flag_no_info = F.col(job_main_resp1).rlike(
        r"^\s*$|^$|^N+[/\ ]*[AONE]+[ N/\AONE]*$|^NA[ MB]*A$|^NA NIL$|^NA N[QS]$|^NOT *APP[ NOTAP]*$|^[NA ]*NOT *APPLICABLE$|^NOT *APPLICABLE *NOT *APPLICABLE$"  # noqa: E501
    )

    flag_exclude_manage_admin = F.col(job_main_resp1).rlike(
        r"\bPA\b|PERSONAL ASSISTANT|ADVI[SC][EO]R*|BUSINESS|QUALITY|FINANC|INVOIC|PAY(MENT|ROLL)|COMMER|PR[OE]CURE|\bCEO\b|HEAD OF|COMPANY|DIRECT|SUPERVI[SC]|COMPL[YI]|INVENTORY"  # noqa: E501
    ) | F.col(job_main_resp1).rlike(
        r"CO-*ORDIN|MANAG|\bMGR\b|OFFICER|CLER(IC|K)|ANAL*|AUDIT|BANKER|AD+MIN*|ACCOUN|\bH *R\b|HUMAN RESOUO*R[SC]"
    )

    # General exclusions
    flag_exclude_catering = F.col(job_main_resp1).rlike(
        r"CHEF|SOUS|COOK|CATER|BREWERY|CHEESE|KITCHEN|KFC|CULINARY|FARM(ER|ING)"
    )
    flag_exclude_acad_edu = F.col(job_main_resp1).rlike(
        r"AC[AE]DEMIC|RESEAR*CH|SCIEN|LAB(ORATORY)?|DATA|ANAL|STATIST|EPIDEMI|EXAM|EDUCAT|EARLY YEARS|SCHOOL|COLL.GE|TEACH|LECTURE|PROFESS|HOUSE *(M[AI]ST(ER|RESS)|PARENT)|COACH|TRAIN"  # noqa: E501
    ) | F.col(job_main_resp1).rlike(r"INSTRUCT|TUTOR|LEARN")
    flag_exclude_media = F.col(job_main_resp1).rlike(
        r"BROADCAST|JOURNALIST|CAMERA|WRIT|COMMUNICAT|CURAT(OR)*|MARKETING|MUSICIAN|ACT([OE]R|RESS)|ARTIST"
    )
    flag_exclude_retail = F.col(job_main_resp1).rlike(
        r"RETAIL|BUYER|SALE|BUY AND SELL|CUSTOMER|AGENT|BANK(ING|ER)|INSURANCE|BEAUT(Y|ICIAN)?|NAIL|HAIR|SHOP|PROPERTY|TRADE|SUPER *MARKET|WH *SMITH|TESCO"  # noqa: E501
    )
    flag_exclude_domestic = F.col(job_main_resp1).rlike(r"DOMESTIC|CLEAN|LAU*ND.*Y")
    flag_exclude_construction = F.col(job_main_resp1).rlike(
        r"BUILD|CONSTRUCT|RENOVAT|REFIT|ENGINE|PLANT|CR[AI]*NE*|SURVEY(OR)*|DESIGNER|ARCHITECT|TECHNICIAN|MECHAN|MANUFACT|ELECTRIC|CARPENTER"  # noqa: E501
    ) | F.col(job_main_resp1).rlike(
        r"PLUMB|WELD(ER|ING)|PASTER(ER|ING)|\bEE\b|GARDE*N|FURNITURE|MAINT[AIE]*N*[EA]N*CE|\bGAS\b|JOINER"
    )
    flag_exclude_religion = F.col(job_main_resp1).rlike(r"CHAPL[AI]*N|VICAR|CLERGY|MINISTER|PREACH|CHURCH")
    flag_exclude_computing = F.col(job_main_resp1).rlike(
        r"\bI[ \.]*T\.?\b|DIGIT|WEBSITE|NETWORK|DEVELOPER|SOFTWARE|SYSTEM"
    )
    flag_exclude_public_serv = F.col(job_main_resp1).rlike(
        r"CHAIR|CHARITY|CITIZEN|CIVIL|VOLUNT|LIBRAR|TRANSLAT|INVESTIGAT|FIRE ?(WO)?(M[AE]N|FIGHT)|POLICE|POST *(WO)*MAN|PRISON|FIRST AID|SAFETY|\bTAX\b"  # noqa: E501
    ) | F.col(job_main_resp1).rlike(r"LI[CS][EA]N[CS]E|LEA*GAL|LAWYER|\bLAW\b|SO*LICITOR")
    flag_exclude_transport = F.col(job_main_resp1).rlike(
        r"DRIV(E|ER|ING)|PILOT|TRAIN DRIVE|TAXI|LORRY|TRANSPORT|DELIVER|SUPPLY"
    )

    flag_exclude_always = F.col(job_main_resp1).rlike(
        r"AC[AE]DEMIC|LECTURE|DEAN|DOCTOR SCIENCE|DR LAB|DATA ANAL|AC?OUNT(ANT|ANCY)?|WARE *HOUSE|TRADE UNION|SALES (MANAGER|REP)|INVESTIGATION OF+ICE|AC+OUNT|PRISI?ON|DIRECT[OE]R"  # noqa: E501
    )
    flag_include_always = F.col(job_main_resp1).rlike(r"(PALLIATIVE|INTENSIVE) CARE|TRIAGE|CHIROPRACT")

    # Work Status flags
    flag_parental_leave = F.col(job_main_resp1).rlike(
        r"(.A(T|Y)(ERNITY)* LEAVE)|(ADOPTION LEAVE)|(^(.A(T|Y)ERNITY)$)|(ON .A(T|Y)ERNITY)"
    ) & ~F.col(job_main_resp1).rlike(r"(W|Q)ORK(.){0,2}ON .A(T|Y)ERNITY")
    flag_retired = (
        F.col(job_main_resp1).rlike(r".E[TYRF]{1,2}[AIOU][RE][ERWD]")
        & ~F.col(job_main_resp1).rlike(r"SEMI|PART|BEFOR|CARE")
    ) | (
        F.col(job_main_resp1).rlike(r".E[TYRF][AIOU][RE][ERWD]MENT")
        & ~(F.col(job_main_resp1).rlike(r".+.E[TYRF][AIOU][RE][ERWD]MENT|.E[TYRF][AIOU][RE][ERWD]MENT.+"))
        & ~F.col(job_main_resp1).rlike(r"EARLY")
    )
    flag_furlough = F.col(job_main_resp1).rlike(r"FURL") & ~F.col(job_main_resp1).rlike(r"(PART|SEMI).+FURL")
    flag_sick = F.col(job_main_resp1).rlike(r"SICK|I'?LL") & ~(
        F.col(job_main_resp1).rlike(r"(CAR.|WELFARE|PAT|ANIMAL).*SICK|SICK.*(CHILDREN|FAMILIES|PAT|ANIMAL)")
        & ~F.col(job_main_resp1).rlike(r"OFF SICK|SICK LEAVE")
    )
    flag_student = F.col(job_main_resp1).rlike(
        r"(YEAR)\s[:digit:]+|[:digit:]+Y|(6|6\s?TH|SIXTH) FORM|COLL.?EGE|SCHOOL|STUDY|EDUCATION|STUDENT|PUPIL"
    ) & ~(
        F.col(job_main_resp1).rlike(
            r"CARE\s?TAKER|CONSULTANT|FINANCE|EXAM|SENCO|RECEPT|FACILIT|CEO|CLEAN|COMPANY|DIRECTOR|LEARNING SUPPORT|DINNER|COOK|MATRON|JANITOR|LIBRAR|CIVIL SERVANT"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(
            r"CATERING|CHEF|ASSOCIATE|TUTOR|LECTURER|WRITER|TECHNICIAN|DRIV(I|ER)|ASSESSOR|TEACH|ACADEMIC|ACCOUNT|ADMIN|ACTIVITIES|ACCOM|ADULT|AFTER|YOUTH|MANAGER|COACH|PRINCIPAL|HEAD|PROF|WORK|ASSIST|OFFICE"  # noqa: E501
        )
    )
    flag_apprentice = F.col(job_main_resp1).rlike(r"AP*RENTI[CS]")
    flag_unemployed = F.col(job_main_resp1).rlike(r"[AEIOU]N.?EMPLOYED|NOT WORKING|LOOKING FOR WORK|JOB.?SEEKING")

    flag_unemployed_ind_columns = (F.col(job_title1).rlike(r"^NO?NE$|^N(O$|O\s)") is True) | (
        F.col(main_resp1).rlike(r"^NO?NE$") is True
    )

    # Patient Facing Flags
    flag_non_patient_facing = (
        flag_admin
        | flag_secretary
        | flag_call_handler
        | F.col(job_main_resp1).rlike(
            r"ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL|(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(
            r"(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)|SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? PRESCRI"  # noqa: E501
        )
    ) & ~F.col(job_main_resp1).rlike(r"(?<!NOT )OFFICE BASED")
    flag_patient_facing = ~flag_non_patient_facing & (
        F.col(job_main_resp1).rlike(
            r"PALLIATIVE CARE|(?<!NOT )PATI[EA]NT FACING|(LOOK(S|ING)? AFTER|SEES?|CAR(E|ING) (OF|FOR)) PATI[EA]NTS|(?<!NO )FACE TO FACE|(?<!NOT )FACE TO FACE"  # noqa: E501
        )
        | F.col(job_main_resp1).rlike(
            r"(?<!NO )(DIRECT )?CONTACT WITH PATI[EA]NTS|CLIENTS COME TO (HER|HIS|THEIR) HOUSE"
        )
        | flag_paramedic
        | flag_additional_hc
        | flag_covid_test
    )
    flag_nhc = F.when(
        (
            flag_exclude_transport
            | flag_exclude_catering
            | flag_exclude_acad_edu
            | flag_exclude_media
            | flag_exclude_retail
            | flag_exclude_domestic
            | flag_exclude_construction
            | flag_exclude_religion
            | flag_exclude_computing
            | flag_exclude_public_serv
            | flag_nhc_admin
            | flag_nhc_secretary
            | flag_nhc_receptionist
            | flag_nhc_counsellor
            | flag_nhc_support
            | flag_nhc_psychologist
            | flag_vet
            | flag_nhc_pharmacist
            | flag_house_care
            | flag_child_care
            | flag_formal_care
            | flag_informal_care
            | flag_social_worker
        ),
        True,
    ).otherwise(False)
    flag_hc = F.when(
        (
            flag_hc_admin
            | flag_hc_secretary
            | flag_hc_receptionist
            | flag_hc_counsellor
            | flag_hc_support
            | flag_hc_pharmacist
            | flag_call_handler
            | flag_patient_facing
            | flag_dietician
            | flag_doctor
            | flag_dentist
            | flag_midwife
            | flag_nurse
            | flag_paramedic
            | flag_physiotherapist
        ),
        True,
    ).otherwise(False)

    healthcare_bin = (
        F.when((flag_nhc & ~flag_hc) | flag_vet | flag_exclude_always, "No")
        .when(
            (flag_hc & ~flag_nhc)
            | flag_include_always
            | flag_additional_hc
            | flag_midwife
            | flag_paramedic
            | flag_doctor
            | flag_nurse
            | flag_covid_test
            | (flag_dentist & ~(flag_exclude_transport | flag_exclude_computing | flag_exclude_retail)),
            "Yes",
        )
        .when((flag_hc & flag_nhc) | flag_exclude_manage_admin, "No")
        .when(
            flag_no_info | F.col(job_main_resp1).isNull(),
            None,
        )
        .otherwise("No")
    )
    patient_facing_final = (
        F.when(F.col(healthcare_bin) == "No", "Not healthcare")
        .when(F.col(healthcare_bin).isNull(), None)
        .when(flag_non_patient_facing, "No")
        .when(flag_patient_facing, "Yes")
        .when(flag_hc_receptionist | flag_hc_counsellor | flag_hc_support, "No")
        .when(
            flag_dietician
            | flag_doctor
            | flag_dentist
            | flag_midwife
            | flag_nurse
            | flag_paramedic
            | flag_pharmacist
            | flag_physiotherapist,
            "Yes",
        )
        .when(F.col(patient_facing_orig) == "non-patient-facing", "No")
        .when(F.col(patient_facing_orig) == "patient-facing", "Yes")
        .when(F.col(work_direct_contact_patients_etc) == "No", "No")
        .when(F.col(work_direct_contact_patients_etc) == "Yes", "Yes")
    )
    work_status_final = (
        F.when(
            (
                flag_furlough
                | flag_retired
                | flag_parental_leave
                | flag_sick
                | flag_unemployed
                | flag_unemployed_ind_columns
            ),
            "not_working",
        )  # Maybe add '& !(flag_partial)'
        .when(flag_student | F.col(work_status) == "Student", "student")
        .when(["Employed", "Self-employed"] in F.col(work_status) | flag_apprentice, "working")
        .when(
            ["Furloughed (temporarily not working)", "Not working (unemployed, retired, long-term sick etc.)"]
            in F.col(work_status),
            "not_working",
        )
        .otherwise(None)
    )

    pf_bin = F.when(patient_facing_final == "Yes", "Yes").otherwise("No")
    pfYes = F.when(pf_bin == "Yes", F.count(pf_bin))
    pfNo = F.when(pf_bin == "No", F.count(pf_bin))

    pfYes_perc = pfYes / (pfYes + pfNo)
    pf_ever_never_20_perc = pfYes_perc >= 0.2

    return (
        healthcare_bin,
        patient_facing_final,
        pf_ever_never_20_perc,
        work_status_final,
    )
