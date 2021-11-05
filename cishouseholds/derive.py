import re
from itertools import chain
from typing import List

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assign_work_social_column(
    df: DataFrame, column_name_to_assign: str, work_sector_colum: str, care_home_column: str, direct_contact_column: str
):
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
        F.when(F.col(work_sector_colum).isNull(), None)
        .when(F.col(work_sector_colum) != "Furloughed (temporarily not working)", "No")
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


def assign_unique_id_column(df: DataFrame, column_name_to_assign: str, concat_columns: List[str]):
    """
    Assign a unique column from concatenating multiple input columns
        concat_columns
    """
    return df.withColumn(column_name_to_assign, F.concat(*concat_columns))


def assign_has_been_to_column(
    df: DataFrame, column_name_to_assign: str, contact_participant_column: str, contact_other_column: str
):
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


def assign_covid_contact_status(df: DataFrame, column_name_to_assign: str, known_column: str, suspect_column: str):
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
    Parameters
    ----------
    df
    column_name_to_assign
    """
    return df.withColumn(column_name_to_assign, F.input_file_name())


def assign_column_from_mapped_list_key(df: DataFrame, column_name_to_assign: str, reference_column: str, map: dict):
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


def assign_test_target(df: DataFrame, column_name_to_assign: str, filename_column: str):
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


def assign_school_year_september_start(df: DataFrame, dob_column: str, visit_date: str, column_name_to_assign: str):
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
            ((F.month(F.col(visit_date))) >= 9) & ((F.month(F.col(dob_column))) < 9),
            (F.year(F.col(visit_date))) - (F.year(F.col(dob_column))) - 3,
        )
        .when(
            (F.month(F.col(visit_date)) >= 9) | ((F.month(F.col(dob_column))) >= 9),
            (F.year(F.col(visit_date))) - (F.year(F.col(dob_column))) - 4,
        )
        .otherwise((F.year(F.col(visit_date))) - (F.year(F.col(dob_column))) - 5),
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
):
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
):
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
):
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
):
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


def assign_ethnicity_white(df: DataFrame, white_bool_column: str, column_name_to_assign: str):
    """
    Assign string variable for ethnicity white / non-white depending on bool value 0 / 1
    Parameters
    ----------
    df
    white_bool_column
    """
    df = df.withColumn(column_name_to_assign, F.when(F.col(white_bool_column) == 1, "white").otherwise("non-white"))
    return df


def assign_taken_column(df: DataFrame, column_name_to_assign: str, reference_column: str):
    """
    Uses references column value to assign a taken column "yes" or "no" depending on whether
    reference is Null
    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(column_name_to_assign, F.when(F.col(reference_column).isNull(), "no").otherwise("yes"))

    return df


def assign_outward_postcode(df: DataFrame, column_name_to_assign: str, reference_colum: str):
    """
    Assign column outer postcode with cleaned data from reference postcode column.
    take only left part of postcode and capitalise
    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    """
    df = df.withColumn(column_name_to_assign, F.upper(F.split(reference_colum, " ").getItem(0)))
    df = df.withColumn(
        column_name_to_assign, F.when(F.length(column_name_to_assign) > 4, None).otherwise(F.col(column_name_to_assign))
    )

    return df


def assign_column_from_coalesce(df: DataFrame, column_name_to_assign: str, *args):
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


def substring_column(df: DataFrame, new_column_name, column_to_substr, start_position, len_of_substr):
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
    df = df.withColumn(new_column_name, F.substring(column_to_substr, start_position, len_of_substr))

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


def derive_cq_pattern(df: DataFrame, column_names, spark_session):
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


def mean_across_columns(df: DataFrame, new_column_name: str, column_names: list):
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
    df: DataFrame, column_name_to_assign: str, start_reference_column: str, end_reference_column: str
):
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

    Return
    ------
    pyspark.sql.DataFrame
    """
    return df.withColumn(
        column_name_to_assign, F.datediff(end=F.col(end_reference_column), start=F.col(start_reference_column))
    )


def assign_column_uniform_value(df: DataFrame, column_name_to_assign: str, uniform_value):
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


def assign_column_regex_match(df: DataFrame, column_name_to_assign: str, reference_column: str, pattern: str):
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


def assign_consent_code(df: DataFrame, column_name_to_assign: str, reference_columns: list):
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


def assign_column_to_date_string(df: DataFrame, column_name_to_assign: str, reference_column: str):
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

    Returns
    -------
    pyspark.sql.DataFrame
    """

    return df.withColumn(column_name_to_assign, F.date_format(F.col(reference_column), "yyyy-MM-dd"))


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


def assign_isin_list(df: DataFrame, column_name_to_assign: str, reference_column_name: str, values_list: list):
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

    Return
    ------
    pyspark.sql.DataFrame
    """
    return df.withColumn(
        column_name_to_assign,
        F.when((F.col(reference_column_name).isin(values_list)), 1)
        .when((~F.col(reference_column_name).isin(values_list)), 0)
        .otherwise(None),
    )


def assign_from_lookup(df: DataFrame, column_name_to_assign: str, reference_columns: list, lookup_df: DataFrame):
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


def assign_age_at_date(df: DataFrame, column_name_to_assign: str, base_date, date_of_birth):
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
        column_name_to_assign, F.floor(F.col("date_diff") / 365.25)
    )

    return df.drop("date_diff")


def assign_correct_age_at_date(df: DataFrame, column_name_to_assign, reference_date, date_of_birth):
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
