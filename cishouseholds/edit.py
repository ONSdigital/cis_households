from itertools import chain
from typing import List
from typing import Mapping
from typing import Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pyspark_utils import get_or_create_spark_session

#######################################################################################################


def update_symptoms_last_7_days_any(df: DataFrame, column_name_to_update: str, count_reference_column: str):
    """
    update value to no if symptoms are ongoing
    Parameters
    ----------
    df
    column_name_to_update
    count_reference_column
    """
    df = df.withColumn(
        column_name_to_update, F.when(F.col(count_reference_column) > 0, "No").otherwise(F.col(column_name_to_update))
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


def clean_barcode(df: DataFrame, barcode_column: str) -> DataFrame:
    """
    Clean lab sample barcodes.
    Converts barcode start to 'ONS' if not a valid variant. Removes barcodes with only 0 values in numeric part or not
    matching the expected format.
    """
    df = df.withColumn(barcode_column, F.upper(F.regexp_replace(F.col(barcode_column), " ", "")))
    df = df.withColumn(
        barcode_column,
        F.when(
            F.col(barcode_column).rlike(r"^(?!ONS|ONW|ONC|ONN)\w{3}\d{8}$"),
            F.regexp_replace(barcode_column, r"^\w{3}", "ONS"),
        ).otherwise(F.col(barcode_column)),
    )
    df = df.withColumn(
        barcode_column,
        F.when(F.col(barcode_column).rlike(r"^\w{3}(?!0{8})\d{8}$"), F.col(barcode_column)).cast("string"),
    )
    return df


def clean_postcode(df: DataFrame, postcode_column: str):
    """
    update postcode variable to include only uppercase alpha numeric characters and set
    to null if required format cannot be identified
    Parameters
    ----------
    df
    postcode_column
    """
    df = df.withColumn(
        postcode_column,
        F.upper(F.ltrim(F.rtrim(F.regexp_replace(postcode_column, r"[^a-zA-Z\d:]", "")))),
    )
    df = df.withColumn("TEMP", F.substring(df[postcode_column], -3, 3))
    df = df.withColumn(postcode_column, F.regexp_replace(postcode_column, r"[^*]{3}$", ""))
    df = df.withColumn(
        postcode_column,
        F.when(
            (F.length(postcode_column) <= 4), F.format_string("%s %s", F.col(postcode_column), F.col("TEMP"))
        ).otherwise(None),
    )
    return df.drop("TEMP")


def update_from_csv_lookup(df: DataFrame, csv_filepath: str, id_column: str):
    """
    Update specific cell values from a map contained in a csv file
    Parameters
    ----------
    df
    csv_filepath
    id_column
        column in dataframe containing unique identifier
    """
    spark = get_or_create_spark_session()
    csv = spark.read.csv(csv_filepath, header=True)
    csv = csv.groupBy("id", "old", "new").pivot("column").count()
    cols = csv.columns[3:]
    for col in cols:
        copy = csv.filter(F.col(col) == 1)
        copy = copy.drop(col).withColumnRenamed("old", col)
        df = df.join(copy.select("id", "new", col), on=["id", col], how="left")
        df = df.withColumn(col, F.when(~F.col("new").isNull(), F.col("new")).otherwise(F.col(col))).drop("new")
    return df


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


def update_column_values_from_map(df: DataFrame, column: str, map: dict, error_if_value_not_found=False) -> DataFrame:
    """
    Convert column values matching map key to value
    Parameters
    ----------
    df
    column
    map
    """
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
            column, F.when(F.col(column).isin(*list(map.keys())), mapping_expr[df[column]]).otherwise(F.col(column))
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
    assert len(key_types) == 1, f"all map keys must be the same type, they are {key_types}"
    assert len(value_types) == 1, f"all map values must be the same type, they are {value_types}"

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
