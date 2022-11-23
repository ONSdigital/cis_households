import re
from functools import reduce
from itertools import chain
from operator import add
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.expressions import all_columns_null
from cishouseholds.expressions import any_column_not_null
from cishouseholds.expressions import any_column_null
from cishouseholds.expressions import count_occurrence_in_row
from cishouseholds.expressions import set_date_component
from cishouseholds.expressions import sum_within_row


def correct_date_ranges_union_dependent(
    df: DataFrame, columns_to_edit: List[str], participant_id_column: str, visit_date_column: str
):
    """
    Correct datetime columns given a range looking across all rows
    """
    for col in columns_to_edit:
        df = df.withColumn("MONTH", F.month(df[col])).withColumn("DAY", F.dayofmonth(df[col]))
        date_ref = (
            df.withColumn(col, F.when(F.col(col) <= F.col(visit_date_column), F.col(col)))
            .select(participant_id_column, col, "MONTH", "DAY")
            .filter((~any_column_null([col, "MONTH", "DAY"])))
            .distinct()
        )

        joined_df = df.join(
            date_ref.withColumnRenamed(col, f"{col}_ref"),
            [participant_id_column, "MONTH", "DAY"],
            how="left",
        ).filter((F.col(f"{col}_ref") < F.col(visit_date_column)) | (F.col(f"{col}_ref").isNull()))

        joined_df = joined_df.withColumn("DIFF", F.datediff(F.col(visit_date_column), F.col(f"{col}_ref")))
        window = Window.partitionBy(participant_id_column, visit_date_column, col).orderBy("DIFF")
        joined_df = joined_df.withColumn("ROW", F.row_number().over(window))
        joined_df = joined_df.filter(F.col("ROW") == 1)

        joined_df = joined_df.withColumn(
            col,
            F.to_timestamp(
                F.when(
                    (F.col(col) > F.col(visit_date_column))
                    & (F.col(col).isNotNull())
                    & ((F.col(f"{col}_ref") <= F.col(visit_date_column)))
                    & (F.col(f"{col}_ref").isNotNull()),
                    F.col(f"{col}_ref"),
                ).otherwise(F.col(col))
            ),
        ).drop(f"{col}_ref")
    return joined_df.drop("MONTH", "DAY", "DIFF", "ROW")


def remove_incorrect_dates(df: DataFrame, columns_to_edit: List[str], visit_date_column: str, min_date: str):
    """
    removes out of range dates
    """
    for col in columns_to_edit:
        df = df.withColumn(col, F.when((F.col(col) < F.col(visit_date_column)) & (F.col(col) > min_date), F.col(col)))
    return df


def correct_date_ranges(df: DataFrame, columns_to_edit: List[str], visit_date_column: str, min_date: str):
    """
    Correct datetime columns given a range
    """
    for col in columns_to_edit:
        df = df.withColumn(
            col,
            F.to_timestamp(
                F.when(
                    (F.col(col) > F.col(visit_date_column)) & (F.col(col).isNotNull()),
                    F.when(F.add_months(col, -1) <= F.col(visit_date_column), F.add_months(col, -1))
                    .when(
                        (F.year(col) - 1 >= 2020) & (F.add_months(col, -12) <= F.col(visit_date_column)),
                        F.add_months(col, -12),
                    )
                    .when(
                        set_date_component(col, "year", F.year(visit_date_column)) <= F.col(visit_date_column),
                        set_date_component(col, "year", F.year(visit_date_column)),
                    )
                    .when(
                        (F.month(col) >= 8) & (set_date_component(col, "year", 2019) <= F.col(visit_date_column)),
                        set_date_component(col, "year", 2019),
                    )
                    .otherwise(F.col(col)),
                )
                .when(
                    (F.col(col) < min_date) & (F.col(col).isNotNull()),
                    F.when(
                        set_date_component(col, "year", F.year(visit_date_column)) <= F.col(visit_date_column),
                        set_date_component(col, "year", F.year(visit_date_column)),
                    )
                    .when(F.add_months(col, 1) <= F.col(visit_date_column), F.add_months(col, 1))
                    .when((F.year(col) > 2019), set_date_component(col, "year", F.year(visit_date_column) - 1))
                    .otherwise(F.col(col)),
                )
                .otherwise(F.col(col))
            ),
        )
    return df


def clean_job_description_string(df: DataFrame, column_name_to_assign: str):
    """
    Remove non alphanumeric characters and duplicate spaces from work main job role variable and set to uppercase.
    Also removes NA type responses.

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_assign
        The name of the column to edit
    """
    cleaned_string = F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(F.regexp_replace(F.upper(F.col(column_name_to_assign)), r"-", " "), r"\s{2,}", " "),
            r"([^a-zA-Z0-9&\s]{1,})|(^\s)|(\s$)",
            "",
        ),
        r"^N+[/\ ]*[AONE]+[ N/\\AONE]*$|^NA[ MB]*A$|^NA NIL$|^NA N[QS]$|^NOT *APP[ NOTAP]*$|^[NA ]*NOT APPLICABLE$|^(NOT APPLICABLE ?)*$",  # noqa: E501
        "",
    )

    df = df.withColumn(column_name_to_assign, F.when(cleaned_string != "", cleaned_string))
    return df


def update_strings_to_sentence_case(df: DataFrame, columns: List[str]):
    """
    Apply lower case to all but first letter of string in list of columns

    Parameters
    ----------
    df
        The input DataFrame to process
    columns
        A list of columns to apply the editing described above
    """
    for col in columns:
        df = df.withColumn(
            col,
            F.concat(
                F.upper(F.substring(F.col(col), 0, 1)),
                F.lower(F.expr(f"substring({col}, 2, length({col})-1)")),
            ),
        )
    return df


def update_column_in_time_window(
    df: DataFrame, column_name_to_update: str, time_column: str, new_value: Any, time_window: List[str]
):
    """
    Update the value of a column to a fixed value if the time the participant filled out the survey exists in a window

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update/edit
    time_column
        The name of the timestamp column
    new_value
        The new value to insert into `column_name_to_update` column
    time_window
        A list of two timestamps given as a string eg: ["2020-01-09T12:00:00", "2020-12-09T12:00:00"]. First timestamp
        must be older than the second timestamp in this list.
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            (F.col(time_column) > F.to_timestamp(F.lit(time_window[0])))
            & (F.col(time_column) < F.to_timestamp(F.lit(time_window[1]))),
            new_value,
        ).otherwise(F.col(column_name_to_update)),
    )
    return df


def update_to_value_if_any_not_null(df: DataFrame, column_name_to_assign: str, value_to_assign: str, column_list: list):
    """Edit existing column to `value_to_assign` when a value is present in any of the listed columns.

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_assign
        The name of the existing column
    value_to_assign
        The value to assign
    column_list
        A list of columns to check if any of them do not have null values
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(any_column_not_null(column_list), value_to_assign).otherwise(F.col(column_name_to_assign)),
    )
    return df


def update_column_if_ref_in_list(
    df: DataFrame,
    column_name_to_update: str,
    old_value,
    new_value,
    reference_column: str,
    check_list: List[Union[str, int]],
):
    """
    Update column value with new_value if the current value is equal to old_value
    and reference column is in list

    Parameters
    ----------
    df
    column_name_to_update
    old_value
    new_value
    reference_column:str
    check_list
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(column_name_to_update).eqNullSafe(old_value) & F.col(reference_column).isin(check_list), new_value
        ).otherwise(F.col(column_name_to_update)),
    )
    return df


def update_value_if_multiple_and_ref_in_list(
    df: DataFrame,
    column_name_to_update: str,
    check_list: List[str],
    new_value_if_in_list: str,
    new_value_if_not_in_list: str,
    separator: str,
):
    """
    Update column value with new value if multiple strings found, separated by separator e.g. ','
    and based on whether column contains any value in check_list or not

    Parameters
    -----------
    df
    column_name_to_update
    check_list
    new_value_if_in_list
    new_value_if_not_in_list
    separator
    """
    df = df.withColumn("ref_flag", F.lit(0))
    for check in check_list:
        df = df.withColumn(
            "ref_flag",
            F.when(
                (F.col(column_name_to_update).contains(separator)) & (F.col(column_name_to_update).contains(check)),
                F.col("ref_flag") + F.lit(1),
            ).when(
                (F.col(column_name_to_update).contains(separator)) & ~(F.col(column_name_to_update).contains(check)),
                F.col("ref_flag"),
            ),
        )

    df = df.withColumn(
        column_name_to_update,
        F.when(F.col("ref_flag") >= F.lit(1), new_value_if_in_list)
        .when(F.col("ref_flag") < F.lit(1), new_value_if_not_in_list)
        .otherwise(F.col(column_name_to_update)),
    ).drop(F.col("ref_flag"))
    return df


# SUBSTITUTED by update_column_values_from_map()
# def update_column_values_from_column_reference(
#     df: DataFrame, column_name_to_update: str, reference_column: str, map: Mapping
# ):
#     """
#     Map column values depending on values of reference columns
#     Parameters
#     ----------
#     df
#     column_name_to_update
#     reference_column
#     map
#     """
#     for key, val in map.items():
#         df = df.withColumn(
#             column_name_to_update, F.when(F.col(reference_column) == key, val).otherwise(F.col(column_name_to_update))
#         )
#     return df


def clean_within_range(df: DataFrame, column_name_to_update: str, range: List[int]) -> DataFrame:
    """
    Convert values outside range to null

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update
    range
        A list of two numbers - 1st number in this list must be less than the 2nd number
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            (F.col(column_name_to_update) >= range[0]) & (F.col(column_name_to_update) <= range[1]),
            F.col(column_name_to_update),
        ).otherwise(None),
    )
    return df


def update_person_count_from_ages(df: DataFrame, column_name_to_assign: str, column_pattern: str):
    """
    Update a count to the count of columns that have a value above 0. Keeps original value if count is not more than 0.

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update
    column_pattern
        regex pattern to select columns that should be counted

    """
    r = re.compile(column_pattern)
    columns_to_count = list(filter(r.match, df.columns))
    count = reduce(add, [F.when(F.col(column) > 0, 1).otherwise(0) for column in columns_to_count])
    df = df.withColumn(
        column_name_to_assign,
        F.when(count > 0, count)
        .when(F.col(column_name_to_assign).isNull(), 0)
        .otherwise(F.col(column_name_to_assign))
        .cast("integer"),
    )
    return df


def update_face_covering_outside_of_home(
    df: DataFrame, column_name_to_update: str, covered_enclosed_column: str, covered_work_column: str
):
    """
    Update the face covering variable by using a lookup to set value of cell based upon values of 2 other columns

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update
    covered_enclosed_column
        Name of the column capturing wether the person wears a mask in enclosed spaces
    covered_work_column
        Name of the column capturing wether the person wears a mask in work setting
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            (
                (
                    F.col(covered_enclosed_column).isin(
                        "Never", "Not going to other enclosed public spaces or using public transport"
                    )
                )
                & (F.col(covered_work_column).isin(["Never", "Not going to place of work or education"]))
            ),
            "No",
        )
        .when(
            (
                ~(F.col(covered_enclosed_column).isin(["Yes, sometimes", "Yes, always", "My face is already covered"]))
                | F.col(covered_enclosed_column).isNull()
            )
            & F.col(covered_work_column).isin(["Yes, sometimes", "Yes, always"]),
            "Yes, at work/school only",
        )
        .when(
            (F.col(covered_enclosed_column).isin(["Yes, sometimes", "Yes, always"]))
            & (
                (~F.col(covered_work_column).isin(["Yes, sometimes", "Yes, always", "My face is already covered"]))
                | F.col(covered_work_column).isNull()
            ),
            "Yes, in other situations only",
        )
        .when(
            ~(
                # Don't want them both to have this value, as should result in next outcome if they are
                (F.col(covered_enclosed_column) == "My face is already covered")
                & (F.col(covered_work_column) == "My face is already covered")
            )
            & F.col(covered_enclosed_column).isin(["Yes, sometimes", "Yes, always", "My face is already covered"])
            & F.col(covered_work_column).isin(["Yes, sometimes", "Yes, always", "My face is already covered"]),
            "Yes, usually both Work/school/other",
        )
        .when(
            (F.col(covered_enclosed_column) == "My face is already covered")
            & (
                (~F.col(covered_work_column).isin(["Yes, sometimes", "Yes, always"]))
                | F.col(covered_work_column).isNull()
            ),
            "My face is already covered",
        )
        .when(
            (
                (~F.col(covered_enclosed_column).isin(["Yes, sometimes", "Yes, always"]))
                | F.col(covered_enclosed_column).isNull()
            )
            & (F.col(covered_work_column) == "My face is already covered"),
            "My face is already covered",
        )
        .otherwise(F.col(column_name_to_update)),
    )
    return df


def update_think_have_covid_symptom_any(df: DataFrame, column_name_to_update: str):
    """
    Update value to no if count of original 12 symptoms is 0 otherwise set to Yes

    Parameters
    ----------
    df
    column_name_to_update
    """
    original_symptoms = [
        "think_have_covid_symptom_muscle_ache_myalgia",
        "think_have_covid_symptom_fatigue_weakness",
        "think_have_covid_symptom_sore_throat",
        "think_have_covid_symptom_shortness_of_breath",
        "think_have_covid_symptom_nausea_vomiting",
        "think_have_covid_symptom_abdominal_pain",
        "think_have_covid_symptom_diarrhoea",
        "think_have_covid_symptom_loss_of_smell",
        "think_have_covid_symptom_loss_of_taste",
        "think_have_covid_symptom_headache",
        "think_have_covid_symptom_cough",
        "think_have_covid_symptom_fever",
    ]
    df = df.withColumn(
        column_name_to_update, F.when(count_occurrence_in_row(original_symptoms, "Yes") > 0, "Yes").otherwise("No")
    )
    return df


def clean_barcode_simple(df: DataFrame, barcode_column: str):
    """
    Clean barcode by converting to upper an removing whitespace
    """
    df = df.withColumn(barcode_column, F.upper(F.regexp_replace(F.col(barcode_column), r"[^a-zA-Z0-9]", "")))
    return df


def clean_barcode(df: DataFrame, barcode_column: str, edited_column: str) -> DataFrame:
    """
    Clean lab sample barcodes.

    Converts barcode start to 'ONS' if not a valid variant. Removes barcodes with only 0 values in numeric part or not
    matching the expected format.

    Parameters
    ---------
    df
        The input DataFrame to process
    barcode_column
        Name of the column containing the barcode
    edited_column
        signifies if updating was performed on row
    """
    df = df.withColumn("BARCODE_COPY", F.col(barcode_column))
    df = df.withColumn(barcode_column, F.upper(F.regexp_replace(F.col(barcode_column), r"[^a-zA-Z0-9]", "")))

    suffix = F.regexp_extract(barcode_column, r"[\dOI]{1,8}$", 0)
    prefix = F.regexp_replace(F.col(barcode_column), r"[\dOI]{1,8}$", "")

    # prefix cleaning
    prefix = F.regexp_replace(prefix, r"[0Q]", "O")
    prefix = F.when(~prefix.isin(["ONS", "ONW", "ONC", "ONN"]), F.lit("ONS")).otherwise(prefix)

    # suffix cleaning
    suffix = F.when(F.length(suffix) >= 4, suffix).otherwise(None)
    suffix = F.when(suffix.rlike(r"^0{1,}$"), None).otherwise(suffix)
    suffix = F.regexp_replace(suffix, r"[.O]", "0")
    suffix = F.regexp_replace(suffix, "I", "1")
    suffix = F.substring(F.concat(F.lit("00000000"), suffix), -8, 8)
    suffix = F.regexp_replace(suffix, r"^[^027]", "0")

    df = df.withColumn(barcode_column, F.when(suffix.isNotNull(), F.concat(prefix, suffix)).otherwise(None))
    df = df.withColumn(
        edited_column, F.when(~F.col("BARCODE_COPY").eqNullSafe(F.col(barcode_column)), 1).otherwise(None)
    )
    return df.drop("BARCODE_COPY")


def clean_postcode(df: DataFrame, postcode_column: str):
    """
    Update postcode variable to include only uppercase alpha numeric characters and set
    to null if required format cannot be identified.

    Parameters
    ----------
    df
        The input DataFrame to process
    postcode_column
        Name of the column containing the postcode to clean
    """
    cleaned_postcode_characters = F.upper(F.regexp_replace(postcode_column, r"[^a-zA-Z\d]", ""))
    inward_code = F.substring(cleaned_postcode_characters, -3, 3)
    outward_code = F.regexp_replace(cleaned_postcode_characters, r".{3}$", "")
    df = df.withColumn(
        postcode_column,
        F.when(F.length(outward_code) <= 4, F.concat(F.rpad(outward_code, 4, " "), inward_code)).otherwise(None),
    )
    return df


def update_from_lookup_df(df: DataFrame, lookup_df: DataFrame, id_column: str = None, dataset_name: str = None):
    """
    Edit values in df based on old to new mapping in lookup_df

    Expected columns on lookup_df:
    - id_column_name
    - id
    - dataset_name
    - target_column_name
    - old_value
    - new_value

    Parameters
    ----------
    df
        The input DataFrame to process
    lookup_df
        The lookup df with the structure described
    dataset_name
        Name of the dataset to filter rows in `lookup_df` by
    id_column
        Name of the the id column in `df`
    """
    drop_list = []
    id_columns = [id_column]
    if dataset_name is not None:
        lookup_df = lookup_df.filter(F.col("dataset_name") == dataset_name)

    if id_column is None:
        id_columns = list(lookup_df.select("id_column_name").distinct().toPandas()["id_column_name"])

    for id_column in id_columns:
        temp_lookup_df = lookup_df.filter(F.col("id_column_name") == id_column)
        columns_to_edit = list(temp_lookup_df.select("target_column_name").distinct().toPandas()["target_column_name"])
        pivoted_lookup_df = (
            temp_lookup_df.groupBy("id")
            .pivot("target_column_name")
            .agg(
                F.first("old_value").alias(f"{id_column}_old_value"),
                F.first("new_value").alias(f"{id_column}_new_value"),
            )
            .drop(f"{id_column}_old_value", f"{id_column}_new_value")
        )
        df = df.join(pivoted_lookup_df, on=(pivoted_lookup_df["id"] == df[id_column]), how="left").drop(
            pivoted_lookup_df["id"]
        )

        for column_to_edit in columns_to_edit:
            if column_to_edit not in df.columns:
                print(
                    f"WARNING: Target column to edit, from editing lookup, does not exist in dataframe: {column_to_edit}"
                )  # functional
                continue
            df = df.withColumn(
                column_to_edit,
                F.when(
                    F.col(column_to_edit).eqNullSafe(
                        F.col(f"{column_to_edit}_{id_column}_old_value").cast(df.schema[column_to_edit].dataType)
                    ),
                    F.col(f"{column_to_edit}_{id_column}_new_value").cast(df.schema[column_to_edit].dataType),
                ).otherwise(F.col(column_to_edit)),
            )

        for barcode_column, correct_col in zip(
            ["swab_sample_barcode_user_entered", "blood_sample_barcode_user_entered"],
            ["swab_sample_barcode_correct", "blood_sample_barcode_correct"],
        ):
            if all(col in df.columns for col in [barcode_column, correct_col]):
                df = df.withColumn(
                    correct_col,
                    F.when(
                        (F.col(f"{barcode_column}_{id_column}_old_value").isNull())
                        & (F.col(f"{barcode_column}_{id_column}_new_value").isNotNull()),
                        "No",
                    )
                    .when(
                        (F.col(f"{barcode_column}_{id_column}_old_value").isNotNull())
                        & (F.col(f"{barcode_column}_{id_column}_new_value").isNull()),
                        "Yes",
                    )
                    .when(
                        (F.col(f"{barcode_column}_{id_column}_old_value").isNotNull())
                        & (F.col(f"{barcode_column}_{id_column}_new_value").isNotNull()),
                        "No",
                    )
                    .otherwise(F.col(correct_col)),
                )
        drop_list.extend(
            [
                *[f"{col}_{id_column}_old_value" for col in columns_to_edit],
                *[f"{col}_{id_column}_new_value" for col in columns_to_edit],
            ]
        )

    return df.drop(*drop_list)


def split_school_year_by_country(df: DataFrame, school_year_column: str, country_column: str):
    """
    Create separate columns for school year depending on the individual's country of residence

    Parameters
    ----------
    df
        The input DataFrame to process
    school_year_column
        The column containing school year info
    country_column
        The column containing country of residence info
    """
    countries = [["England", "Wales"], ["Scotland"], ["Northern Ireland"]]
    column_names = ["school_year_england_wales", "school_year_scotland", "school_year_northern_ireland"]
    for column_name, country_set in zip(column_names, countries):
        df = df.withColumn(
            column_name, F.when(F.col(country_column).isin(country_set), F.col(school_year_column)).otherwise(None)
        )
    return df


def update_social_column(df: DataFrame, social_column: str, health_column: str):
    """
    Update the value of the social column to that of the health column
    provided that the social column is null and health column is not

    Parameters
    ----------
    df
    social_column
    health_column
    """
    df = df.withColumn(
        social_column,
        F.when((F.col(social_column).isNull()) & (~F.col(health_column).isNull()), F.col(health_column)).otherwise(
            F.col(social_column)
        ),
    )
    return df


def update_column_values_from_map(
    df: DataFrame,
    column: str,
    map: dict,
    condition_column: str = None,
    error_if_value_not_found: Optional[bool] = False,
    default_value: Union[str, bool, int] = None,
) -> DataFrame:
    """
    Given a map (dictionary) of Key-Value pairs, Replace column values that match the Keys
    in the map/dictionary with the corresponding Values.

    Parameters
    ----------
    df
        The input DataFrame to process
    column
        The column name to assign - alias for column_name_to_update
    map
        A dictionary of dictionaries - the top level key in this dictionary can correspond to
        the `column` you want to update. A dictionary associated with the top level key is expected
        contain key-value pairs. The keys in the key-value pairs are matched with the values in
        the column `column` and when matched, the value in the column is replaced by the value in
        corresponding key-value pair.
    condition_column
        The column containing the value to be mapped using mapping_expr
    error_if_value_not_found
        If True, an error is raised if the set of values to map are not present in `map`
    default_value
        Default value to use when values in column `column` cannot be matched with keys in `map`
    """
    if condition_column is None:
        condition_column = column

    if default_value is None:
        default_value = F.col(column)

    mapping_expr = F.create_map([F.lit(x) for x in chain(*map.items())])  # type: ignore
    if error_if_value_not_found:
        temp_df = df.distinct()
        values_set = set(temp_df.select(column).toPandas()[column].tolist())
        map_set = set(map.keys())
        if map_set != values_set:
            missing = set(temp_df.select(column).toPandas()[column].tolist()) - set(map.keys())
            raise LookupError(f"Insufficient mapping values: contents of:{missing} remains unmapped")
        df = df.withColumn(column, mapping_expr[df[column]])
    else:
        df = df.withColumn(
            column,
            F.when(F.col(condition_column).isin(*list(map.keys())), mapping_expr[df[condition_column]]).otherwise(
                default_value
            ),
        )
    return df


def update_work_facing_now_column(
    df: DataFrame,
    column_name_to_update: str,
    work_status_column: str,
    work_status_list: List[str],
) -> DataFrame:
    """
    Update value of variable depending on state of reference column work_status_column

    Parameters
    ----------
    df
        The input Dataframe to process
    column_name_to_update
        The column to update
    work_status_column
        The column which contains the work status of the participant
    work_status_list
        list of possible work statuses which result in "no" as column to update
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(work_status_column).isin(*work_status_list),
            "No",
        ).otherwise(F.col(column_name_to_update)),
    )
    return df


def convert_null_if_not_in_list(df: DataFrame, column_name: str, options_list: List[str]) -> DataFrame:
    """
    Convert column values to null if the entry is not present in provided list

    Parameters
    ----------
    df
        The Dataframe to process
    column_name
        The column whose values need to be updated
    options_list
        A list of values to compare values in column `column_name` against
    """
    df = df.withColumn(
        column_name, F.when((F.col(column_name).isin(*options_list)), F.col(column_name)).otherwise(None)
    )

    return df


def convert_barcode_null_if_zero(df: DataFrame, barcode_column_name: str):
    """
    Converts barcode to null if numeric characters are all 0 otherwise performs no change

    Parameters
    ----------
    df
        The Dataframe to process
    barcode_column_name
        Name of the column holding the barcode values
    """
    df = df.withColumn(
        barcode_column_name,
        F.when(F.substring(barcode_column_name, 4, 999) == "0" * (F.length(barcode_column_name) - 3), None).otherwise(
            F.col(barcode_column_name)
        ),
    )

    return df


def map_column_values_to_null(df: DataFrame, column_list: List[str], value: str):
    """
    Map columns from column list with given value to null

    Parameters
    ----------
    df
        The Dataframe to process
    column_list
        The list of columns to edit
    value
        The value, if found in any of the columns in `column_list`, to be set to None
    """
    for col in column_list:
        df = df.withColumn(col, F.when(F.col(col) == value, None).otherwise(F.col(col)))
    return df


def convert_columns_to_timestamps(df: DataFrame, column_format_map: dict) -> DataFrame:
    """
    Convert string columns to timestamp given format.

    Parameters
    ----------
    df
    column_format_map
        format of datetime string and associated list of column names to which it applies
    """
    for format, columns_list in column_format_map.items():
        for column_name in columns_list:
            if column_name in df.columns:
                df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), format=format))

    return df


def apply_value_map_multiple_columns(df: DataFrame, column_map_dic: Mapping):
    """A wrapper around update_column_values_from_map function.

    Parameters
    ----------
    df
        The Dataframe to process
    column_map_dic
        A dictionary with column name (to edit) as Key and the value being another dictionary, whose
        keys are the values we want to replace by the corresponding value in the Key:Value pair

    """
    for col, map in column_map_dic.items():
        df = update_column_values_from_map(df, col, map)
    return df


def format_string_upper_and_clean(df: DataFrame, column_name_to_assign: str) -> str:
    """
    Remove all instances of whitespace before and after a string field including all duplicate spaces
    along with dots (.) as well

    Parameters
    ----------
    df
    column_name_to_assign
    """
    df = df.withColumn(
        column_name_to_assign,
        F.upper(F.ltrim(F.rtrim(F.regexp_replace(column_name_to_assign, r"\s+", " ")))),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            F.substring(column_name_to_assign, -1, 1) == ".",
            F.rtrim(F.col(column_name_to_assign).substr(F.lit(1), F.length(column_name_to_assign) - 1)),
        ).otherwise(F.col(column_name_to_assign)),
    )

    return df


def rename_column_names(df: DataFrame, variable_name_map: dict) -> DataFrame:
    """
    Rename column names.

    Parameters
    ----------
    df
    variable_name_map
        map of current column names to new names
    """
    cleaned_columns = [variable_name_map[old_column_name] for old_column_name in df.columns]
    return df.toDF(*cleaned_columns)


def assign_from_map(df: DataFrame, column_name_to_assign: str, reference_column: str, mapper: Mapping) -> DataFrame:
    """
    Assign column with values based on a dictionary map of reference_column.
    From households_aggregate_processes.xlsx, edit number 1.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be assigned
    reference_column
        Name of column of TimeStamp type to be converted
    mapper
        Dictionary of key value pairs to edit values

    Returns
    ------
    pyspark.sql.DataFrame

    Notes
    -----
    Function works if key and value are of the same type and there is a missing key in the mapper
    If types are the same, the missing keys will be replaced with the reference column value/
    If types are not the same, the missing keys will be given as NULLS
    If key and value are of a different type and there is a missing key in the mapper,
    then the type is not converted.
    """
    key_types = set([type(key) for key in mapper.keys()])
    value_types = set([type(values) for values in mapper.values()])
    assert len(key_types) == 1, f"all map keys must be the same type, they are {key_types} for {column_name_to_assign}"
    assert (
        len(value_types) == 1
    ), f"all map values must be the same type, they are {value_types} for {column_name_to_assign}"

    mapping_expr = F.create_map([F.lit(x) for x in chain(*mapper.items())])

    if key_types == value_types:
        return df.withColumn(
            column_name_to_assign, F.coalesce(mapping_expr[F.col(reference_column)], F.col(reference_column))
        )
    else:
        return df.withColumn(column_name_to_assign, mapping_expr[F.col(reference_column)])


def assign_null_if_insufficient(
    df: DataFrame, column_name_to_assign: str, first_reference_column: str, second_reference_column: str
) -> DataFrame:
    """
    Assign a reference value to null, where two reference columns have specified values.
    Used to null test result values when sample is insufficient.

    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to assign outcome to
    first_reference_column
        Name of column to check for zero value
    second_reference_column
        Name of column to check for insufficient indicator
    """
    return df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(first_reference_column) == 0) & (F.col(second_reference_column) == "Insufficient sample"), None
        ).otherwise(F.col(first_reference_column)),
    )


def edit_swab_results_single(
    df: DataFrame, gene_result_classification: str, gene_result_value: str, overall_result_classification: str
) -> DataFrame:
    """
    The objective of this function is to edit/correct the gene_result_classification from Positive to Negative or 1 to 0
    in case gene_result_value is 0.0 or lower and overall_result_classification is Positive or 1.

    Parameters
    ----------
    df
    gene_result_classification
    gene_result_value
        column name that consists of float values
    overall_result_classification
    """
    return df.withColumn(
        gene_result_classification,
        F.when(
            # boolean logic:
            (F.col(gene_result_classification) == "Positive")
            & (F.col(gene_result_value) <= 0.0)
            & (F.col(overall_result_classification) == "Positive"),
            "Negative"
            # if boolean condition not met, keep the same value.
        ).otherwise(F.col(gene_result_classification)),
    )


def cast_columns_from_string(df: DataFrame, column_list: list, cast_type: str) -> DataFrame:
    """
    Convert string columns to a given datatype.

    Parameters
    ----------
    df
    column_list
        list of columns to be converted
    cast_type
        string containing the datatype for re_casting
    """
    for column_name in column_list:
        if column_name in df.columns:
            df = df.withColumn(column_name, F.col(column_name).cast(cast_type))

    return df


def edit_to_sum_or_max_value(
    df: DataFrame,
    column_name_to_assign: str,
    columns_to_sum: List[str],
    max_value: int,
):
    """
    Imputes column_name_to_assign based a sum of the columns_to_sum.
    If exceeds max, uses max_value. If all values are Null, sets outcome to Null.

    column_name_to_assign must already exist on the df.

    Parameters
    ----------
    df
        The Dataframe to process
    column_name_to_assign
        The column to impute (must already exist in the Dataframe)
    columns_to_sum
        List of column names to sum up
    max_value
        Max value
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(all_columns_null([column_name_to_assign, *columns_to_sum]), None)
        .when(
            F.col(column_name_to_assign).isNull(),
            F.least(F.lit(max_value), sum_within_row(columns_to_sum)),
        )
        .otherwise(F.col(column_name_to_assign))
        .cast("integer"),
    )
    return df


def join_on_existing(df: DataFrame, df_to_join: DataFrame, on: List):
    """
    Join 2 dataframes on columns in 'on' list and
    override empty values in the left dataframe with values from the right
    dataframe.
    """
    columns = [col for col in df_to_join.columns if col in df.columns]
    for col in columns:
        if col not in on:
            df_to_join = df_to_join.withColumnRenamed(col, f"{col}_FT")
    df = df.join(df_to_join, on=on, how="left")
    for col in columns:
        if col not in on:
            df = df.withColumn(col, F.coalesce(F.col(f"{col}_FT"), F.col(col))).drop(f"{col}_FT")
    return df


def fill_nulls(column_name_to_update, fill_value: int = 0):
    """Fill Null and NaN values with a constant integer."""
    return F.when((column_name_to_update.isNull()) | (F.isnan(column_name_to_update)), fill_value).otherwise(
        column_name_to_update
    )


def recode_column_values(df: DataFrame, lookup: dict):
    """wrapper to loop over multiple value maps for different columns"""
    for column_name, map in lookup.items():
        df = update_column_values_from_map(df, column_name, map)
    return df


# 1165
# requires MATCHED_*col
def update_column(df: DataFrame, lookup_df: DataFrame, column_name_to_update: str, join_on_columns: List[str]):
    """
    Assign column (column_name_to_update) new value from lookup dataframe (lookup_df) if the value does not match
    its counterpart in the old dataframe
    """
    lookup_df = lookup_df.withColumnRenamed(column_name_to_update, f"{column_name_to_update}_from_lookup")
    df = df.join(lookup_df, on=[*join_on_columns], how="left")
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.col(column_name_to_update).isNull(),
            F.when(
                F.col(f"{column_name_to_update}_from_lookup").isNotNull(), F.col(f"{column_name_to_update}_from_lookup")
            ).otherwise(("N/A")),
        ).otherwise(F.col(column_name_to_update)),
    )
    return df.drop(f"{column_name_to_update}_from_lookup")


def update_data(df: DataFrame, auxillary_dfs: dict):
    """
    wrapper function for calling update column
    """
    df = update_column(
        df=df,
        lookup_df=auxillary_dfs["postcode_lookup"],
        column_name_to_update="lower_super_output_area_code_11",
        join_on_columns=["country_code_12", "postcode"],
    )
    df = update_column(
        df=df,
        lookup_df=auxillary_dfs["cis_phase_lookup"],
        column_name_to_update="cis_area_code_20",
        join_on_columns=["lower_super_output_area_code_11"],
    )
    return df


def survey_edit_auto_complete(
    df: DataFrame,
    column_name_to_assign: str,
    completion_window_column: str,
    last_question_column: str,
    file_date_column: str,
):
    """
    Add a status type for the variable survey_completion_status to reflect participants who have filled in the final
    question on the survey but had their questionnaire automatically submitted when the test window closed
    as they didn't click "submit"
    """

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col(column_name_to_assign) == "In progress")
            & (F.col(completion_window_column) < F.col(file_date_column))
            & (F.col(last_question_column).isNotNull()),
            "Auto Completed",
        ).otherwise(F.col(column_name_to_assign)),
    )
    return df


def replace_sample_barcode(
    df: DataFrame,
):
    """
    Creates _sample_barcode_combined fields and uses agreed business logic to utilise either the user entered
    barcode (_sample_barcode_user_entered) or the pre-assigned barcode (_sample_barcode)

    Parameters
    ----------
    df : DataFrame
        input dataframe with required sample barcode fields

    Returns
    -------
    df : DataFrame
        output dataframe with replaced sample barcodes
    """

    if "swab_sample_barcode_user_entered" in df.columns:
        for test_type in ["swab", "blood"]:
            df = df.withColumn(
                f"{test_type}_sample_barcode_combined",
                F.when(
                    (
                        (F.col("survey_response_dataset_major_version") == 3)
                        & (F.col(f"{test_type}_sample_barcode_correct") == "No")
                        & ~(F.col(f"{test_type}_sample_barcode_user_entered").isNull())
                    ),
                    F.col(f"{test_type}_sample_barcode_user_entered"),
                ).otherwise(F.col(f"{test_type}_sample_barcode")),
            )
    return df


def conditionally_replace_columns(
    df: DataFrame, column_to_column_map: Dict[str, str], condition: Optional[object] = True
):
    """
    Dictionaries for column_to_column_map are to_replace : replace_with formats.

    Parameters
    ----------
    df : DataFrame
        input df
    column_to_column_map : Dict[str, str]
        to_replace : replace_with
    condition : Optional[object]
        Defaults to True.

    Returns
    -------
    df : DataFrame
        dataframe with replaced column values
    """

    for to_replace, replace_with in column_to_column_map.items():
        df = df.withColumn(to_replace, F.when(condition, F.col(replace_with)).otherwise(F.col(to_replace)))
    return df
