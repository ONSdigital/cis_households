import re
from functools import reduce
from itertools import chain
from operator import add
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.expressions import any_column_not_null
from cishouseholds.expressions import any_column_null
from cishouseholds.expressions import sum_within_row


def update_to_value_if_any_not_null(df: DataFrame, column_name_to_assign: str, value_to_assign: str, column_list: list):
    """Edit existing column to value when a value is present in any of the listed columns."""
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
    convert values outside range to null
    Parameters
    ----------
    df
    column_name_to_update
    range
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
    column_patter
        regex pattern to match columns that should be counted

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
    update the face covering variable by using a lookup to set value of cell based upon values of 2 other columns
    Parameters
    ----------
    df
    column_name_to_update
    covered_enclosed_column
    covered_work_column
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


def update_think_have_covid_symptom_any(df: DataFrame, column_name_to_update: str, count_reference_column: str):
    """
    update value to no if symptoms are ongoing
    Parameters
    ----------
    df
    column_name_to_update
    count_reference_column
    """
    df = df.withColumn(
        column_name_to_update, F.when(F.col(count_reference_column) > 0, "Yes").otherwise(F.col(column_name_to_update))
    )
    return df


def update_visit_order(df: DataFrame, visit_order_column: str) -> DataFrame:
    """
    Ensures visit order row value in list of allowed values
    Parameters
    df
    visit_order_column
    """
    allowed = [
        "First Visit",
        "Follow-up 1",
        "Follow-up 2",
        "Follow-up 3",
        "Follow-up 4",
        "Month 2",
        "Month 3",
        "Month 4",
        "Month 5",
        "Month 6",
        "Month 7",
        "Month 8",
        "Month 9",
        "Month 10",
        "Month 11",
        "Month 12",
        "Month 13",
        "Month 14",
        "Month 15",
        "Month 16",
        "Month 17",
        "Month 18",
        "Month 19",
        "Month 20",
        "Month 21",
        "Month 22",
        "Month 23",
        "Month 24",
    ]
    df = df.withColumn(
        visit_order_column, F.when(F.col(visit_order_column).isin(allowed), F.col(visit_order_column)).otherwise(None)
    )
    return df


def clean_barcode(df: DataFrame, barcode_column: str, edited_column: str) -> DataFrame:
    """
    Clean lab sample barcodes.
    Converts barcode start to 'ONS' if not a valid variant. Removes barcodes with only 0 values in numeric part or not
    matching the expected format.
    Parameters
    ---------
    df
    barcode_column
    edited_column
        signifies if updating was performed on row
    """
    df = df.withColumn("BARCODE_COPY", F.col(barcode_column))
    df = df.withColumn(barcode_column, F.upper(F.regexp_replace(F.col(barcode_column), " ", "")))
    df = df.withColumn(barcode_column, F.regexp_replace(F.col(barcode_column), r"[^a-zA-Z0-9]", ""))

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
    update postcode variable to include only uppercase alpha numeric characters and set
    to null if required format cannot be identified
    Parameters
    ----------
    df
    postcode_column
    """
    cleaned_postcode_characters = F.upper(F.regexp_replace(postcode_column, r"[^a-zA-Z\d]", ""))
    inward_code = F.substring(cleaned_postcode_characters, -3, 3)
    outward_code = F.regexp_replace(cleaned_postcode_characters, r".{3}$", "")
    df = df.withColumn(
        postcode_column,
        F.when(F.length(outward_code) <= 4, F.concat(F.rpad(outward_code, 4, " "), inward_code)).otherwise(None),
    )
    return df


def update_from_lookup_df(df: DataFrame, lookup_df: DataFrame, id_column: str, dataset_name: str = None):
    """
    Edit values in df based on old to new mapping in lookup_df
    Expected columns on lookup_df:
    - id
    - dataset_name
    - target_column_name
    - old_value
    - new_value
    """

    if dataset_name is not None:
        lookup_df = lookup_df.filter(F.col("dataset_name") == dataset_name)

    columns_to_edit = list(lookup_df.select("target_column_name").distinct().toPandas()["target_column_name"])

    pivoted_lookup_df = (
        lookup_df.groupBy("id")
        .pivot("target_column_name")
        .agg(F.first("old_value").alias("old_value"), F.first("new_value").alias("new_value"))
        .drop("old_value", "new_value")
    )
    edited_df = df.join(pivoted_lookup_df, on=(pivoted_lookup_df["id"] == df[id_column]), how="left").drop(
        pivoted_lookup_df["id"]
    )

    for column_to_edit in columns_to_edit:
        if column_to_edit not in df.columns:
            print(
                f"WARNING: Target column to edit, from editing lookup, does not exist in dataframe: {column_to_edit}"
            )  # functional
            continue
        edited_df = edited_df.withColumn(
            column_to_edit,
            F.when(
                F.col(column_to_edit).eqNullSafe(
                    F.col(f"{column_to_edit}_old_value").cast(df.schema[column_to_edit].dataType)
                ),
                F.col(f"{column_to_edit}_new_value").cast(df.schema[column_to_edit].dataType),
            ).otherwise(F.col(column_to_edit)),
        )

    drop_list = [*[f"{col}_old_value" for col in columns_to_edit], *[f"{col}_new_value" for col in columns_to_edit]]
    return edited_df.drop(*drop_list)


def split_school_year_by_country(df: DataFrame, school_year_column: str, country_column: str):
    """
    Create separate columns for school year depending on the individuals country of residence
    Parameters
    ----------
    df
    school_year_column
    country_column
    id_column
    """
    countries = [["England", "Wales"], ["Scotland"], ["NI"]]
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
    Convert column values matching map key to value
    Parameters
    ----------
    df
    column
    map
    error_if_value_not_found
    default_value
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
    column_name_to_update
    work_status_column
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


def dedudiplicate_rows(df: DataFrame, reference_columns: Union[List[str], str]):
    """
    Remove rows based on duplicate values present in reference columns
    Parameters
    ---------
    df
    reference_columns
    """
    if reference_columns == "all":
        return df.distinct()
    else:
        return df.dropDuplicates(reference_columns)


def convert_null_if_not_in_list(df: DataFrame, column_name: str, options_list: List[str]) -> DataFrame:
    """
    Convert column values to null if the entry is no present in provided list
    Parameters
    ----------
    df
    column_name
    options_list
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
    barcode_column_name
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
    column_list
    value
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
    for col, map in column_map_dic.items():
        df = update_column_values_from_map(df, col, map)
    return df


def update_schema_types(schema: dict, column_names: list, new_type: dict):
    """
    Update entries within schema dictionary to reflect a common change across all rows in list (column_names)
    Parameters
    ----------
    schema
    column_names
        list of names of keys within schema to assign new type to
    new_type
        type dictionary to update the schame entry to
    """
    schema = schema.copy()
    for column_name in column_names:
        schema[column_name] = new_type
    return schema


def update_schema_names(schema: dict, column_name_map: dict):
    """
    Update schema dictionary column names using a column name map, of old to new names.
    """
    return {column_name_map[key]: value for key, value in schema.items()}


def format_string_upper_and_clean(df: DataFrame, column_name_to_assign: str) -> str:
    """
    Remove all instances of whitespace before and after a string field including all duplicate spaces
    along with dots (.) aswell
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
    Return
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


def re_cast_column_if_null(df: DataFrame, desired_column_type: str = "integer") -> DataFrame:
    """
    Searches for null type schema in all columns of given dataframe df
    and returns desired format by cast().
    Parameters
    ----------
    df
    desired_column_type
        valid inputs in string: integer, string, double
    """
    for column_name, column_type in df.dtypes:
        if column_type == "null":
            df = df.withColumn(column_name, F.col(column_name).cast(desired_column_type))
    return df


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
    """
    df = df.withColumn(
        column_name_to_assign,
        F.when(any_column_null([column_name_to_assign, *columns_to_sum]), None)
        .when(
            F.col(column_name_to_assign).isNull(),
            F.least(F.lit(max_value), sum_within_row(columns_to_sum)),
        )
        .otherwise(F.col(column_name_to_assign))
        .cast("integer"),
    )
    return df
