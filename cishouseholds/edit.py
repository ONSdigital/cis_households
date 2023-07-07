import re
from functools import reduce
from itertools import chain
from operator import add
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

from cishouseholds.expressions import all_columns_null
from cishouseholds.expressions import any_column_not_null
from cishouseholds.expressions import sum_within_row


def add_prefix(df: DataFrame, column_name_to_update: str, prefix: str, sep: str = ""):
    """Adds a prefix to all the values in a dataframe column

    Parameters
    ----------
    df
       The input DataFrame to process
    column_name_to_update
       The column containing the values the prefix is being added to
    prefix
       The string being fixed to the start of the column values
    sep
       An optional string separate between the prefix and the column value e.g. '_'

    """
    return df.withColumn(column_name_to_update, F.concat_ws(sep, F.lit(prefix), F.col(column_name_to_update)))


def fuzzy_update(
    left_df: DataFrame,
    cols_to_check: List[str],
    update_column: str,
    min_matches: int,
    id_column: str,
    visit_date_column: str,
    right_df: DataFrame = None,
    filter_out_of_range: bool = False,
):
    """
    Update a column value if more than 'min_matches' values match in a series of column values 'cols_to_check'.

    Does not update values that already exist.

    Parameters
    ----------
    left_df
       dataframe to process
    cols_to_check
       columns that will all be checked to see how close in value they are across a window
    update_column
       column to be updated with final resulting value
    min_matches
       minimum number of times values must match in cols_to_check
    id_column
       specifies window over which you are calculating
    visit_date_column
       column containing day of visit
    right_df
       dataframe to join onto left, if different from main df to process
    filter_out_of_range
       Defines whether cols with values greater than visit_date_column will be filtered out or not
    """

    window = Window.partitionBy(id_column).orderBy(id_column)
    specific_window = Window.partitionBy(id_column, "ROW_NUM_LEFT").orderBy(F.desc("TEMP"))
    if right_df is None:
        right_df = left_df
    right_df = right_df.select(id_column, update_column, *cols_to_check).filter(F.col(update_column).isNotNull())

    for c in [c for c in right_df.columns if c != id_column]:
        right_df = right_df.withColumnRenamed(c, f"{c}_right")

    left_df = left_df.withColumn("ROW_NUM_LEFT", F.row_number().over(window))
    right_df = right_df.withColumn("ROW_NUM_RIGHT", F.row_number().over(window))

    df = left_df.join(right_df, on=id_column, how="left")

    # filter for rows where update_column is before the visit
    if filter_out_of_range:
        df = df.filter(F.col(f"{update_column}_right") <= F.col(visit_date_column))

    df = df.withColumn(
        "TEMP",
        reduce(
            add,
            [F.when(F.col(column).eqNullSafe(F.col(f"{column}_right")), 1).otherwise(0) for column in cols_to_check],
        ),
    )
    df = df.withColumn("ROW", F.row_number().over(specific_window))
    df = df.filter((F.col("TEMP") >= min_matches) & (F.col("ROW") == 2)).drop(
        "ROW", *[f"{c}_right" for c in cols_to_check]
    )
    df = left_df.join(
        df.select(id_column, "ROW_NUM_LEFT", f"{update_column}_right"), on=[id_column, "ROW_NUM_LEFT"], how="left"
    ).withColumn(update_column, F.coalesce(F.col(update_column), F.col(f"{update_column}_right")))
    return df.drop("TEMP", "ROW_NUM_LEFT", "ROW_NUM_RIGHT", f"{update_column}_right")


def clean_string_column(df: DataFrame, column_name_to_assign: str):
    """
    Remove non alphanumeric characters and duplicate spaces from a string column and set to uppercase.
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
    df: DataFrame, column_name_to_update: str, event_column: str, new_value: Any, time_window: List[str]
):
    """
    Update the value of a column to a fixed value if an event exists in a window

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update/edit
    event_column
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
            (F.col(event_column) > F.to_timestamp(F.lit(time_window[0])))
            & (F.col(event_column) < F.to_timestamp(F.lit(time_window[1]))),
            new_value,
        ).otherwise(F.col(column_name_to_update)),
    )
    return df


def update_to_value_if_any_not_null(
    df: DataFrame,
    column_name_to_update: str,
    column_list: list,
    default_values: list = [True, False],
):
    """
    Edit existing column to `value_to_assign` when a value is present in any of the listed columns.

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the existing column
    column_list
        A list of columns to check if any of them do not have null values
    default_values
        True and false values to be assigned, with true value taking first position and
        false value taking second position
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(any_column_not_null(column_list), default_values[0]).otherwise(default_values[1]),
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
      The input DataFrame to process
    column_name_to_update
      The name of the existing column
    old_value
      The existing value we want to replace
    new_value
      The value we want to replace old_value with
    reference_column
      The column to check whether contains values in check_list, the presence of
      which decides whether we update with new_value
    check_list
      values in reference_column that will cause the update to column_name_to_update if
      in reference_column
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
    and based on whether column contains any value in check_list or not. This is to reduce
    length of strings and reduce categories

    Parameters
    -----------
     df
      The input DataFrame to process
    column_name_to_update
      The name of the existing column
    check_list
      Values in column_name_to_update to check for, which if exist updates to new_value_if_in_list
    new_value_if_in_list
      column_name_to_update updated to this if check_list condition met
    new_value_if_not_in_list
      column_name_to_update updated to this if check_list condition not met
    separator
      Separator to check for presence of e.g. ;
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


def clean_within_number_range(df: DataFrame, column_name_to_update: str, number_range: List[int]) -> DataFrame:
    """
    Convert values outside range to null

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The name of the column to update
    number_range
        A list of two numbers - 1st number in this list must be less than the 2nd number
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            (F.col(column_name_to_update) >= number_range[0]) & (F.col(column_name_to_update) <= number_range[1]),
            F.col(column_name_to_update),
        ).otherwise(None),
    )
    return df


def update_count_from_columns(df: DataFrame, column_name_to_assign: str, column_pattern: str):
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
        The lookup df with the structure described above
    id_column
        Name of the the id column in `df`
    dataset_name
        Name of the dataset to filter rows in `lookup_df` by
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

        drop_list.extend(
            [
                *[f"{col}_{id_column}_old_value" for col in columns_to_edit],
                *[f"{col}_{id_column}_new_value" for col in columns_to_edit],
            ]
        )

    return df.drop(*drop_list)


def update_null_column_from_target_column(df: DataFrame, column_name_to_update: str, target_column: str):
    """
    Update the value of the social column to that of the health column
    provided that the social column is null and health column is not

    Parameters
    ----------
    df
        The input DataFrame to process
    column_name_to_update
        The column containing social work info
    target_column
        The column containing health work info
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(
            (F.col(column_name_to_update).isNull()) & (~F.col(target_column).isNull()), F.col(target_column)
        ).otherwise(F.col(column_name_to_update)),
    )
    return df


def update_column_values_from_map(
    df: DataFrame,
    column_name_to_update: str,
    map: dict,
    reference_column: str = None,
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
    column_name_to_update
        The column name to assign - alias for column_name_to_update
    map
        A dictionary of dictionaries - the top level key in this dictionary can correspond to
        the `column` you want to update. A dictionary associated with the top level key is expected to
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
    if reference_column is None:
        reference_column = column_name_to_update

    if default_value is None:
        default_value = F.col(column_name_to_update)

    # remove mapped null value
    _map = {k: v for k, v in map.items() if k is not None}

    mapping_expr = F.create_map([F.lit(x) for x in chain(*_map.items())])  # type: ignore
    if error_if_value_not_found:
        temp_df = df.distinct()
        values_set = set(temp_df.select(column_name_to_update).toPandas()[column_name_to_update].tolist())
        map_set = set(map.keys())
        if map_set != values_set:
            missing = set(temp_df.select(column_name_to_update).toPandas()[column_name_to_update].tolist()) - set(
                map.keys()
            )
            raise LookupError(f"Insufficient mapping values: contents of:{missing} remains unmapped")
        df = df.withColumn(
            column_name_to_update,
            F.when(F.col(column_name_to_update).isNull(), map.get(None)).otherwise(
                mapping_expr[df[column_name_to_update]]
            ),
        )
    else:
        df = df.withColumn(
            column_name_to_update,
            F.when(
                (F.col(reference_column).isin(*list(map.keys()))) | (F.col(reference_column).isNull()),
                F.when(F.col(reference_column).isNull(), map.get(None)).otherwise(mapping_expr[df[reference_column]]),
            ).otherwise(default_value),
        )
    return df


def convert_null_if_not_in_list(df: DataFrame, column_name_to_update: str, options_list: List[str]) -> DataFrame:
    """
    Convert column values to null if value not contain in the options_list

    Parameters
    ----------
    df
        The Dataframe to process
    column_name_to_update
        The column whose values need to be updated
    options_list
        A list of values to compare values in column `column_name` against
    """
    df = df.withColumn(
        column_name_to_update,
        F.when((F.col(column_name_to_update).isin(*options_list)), F.col(column_name_to_update)).otherwise(None),
    )

    return df


def nullify_columns_before_date(df: DataFrame, column_list: List[str], date_column: str, date: str):
    """
    Nullify the values of columns in list column_list if the date in the `date_column` is before the specified `date`
    Parameters
    ----------
    df
        The Dataframe to process
    column_list
        list of columns to convert to null if the date_column is < date
    date_column
        The name of the column which is being compared against date
    date
       A date hard coded as agreed business logic, and is compared to date_column

    """
    for col in [c for c in column_list if c in df.columns]:
        df = df.withColumn(col, F.when(F.col(date_column) >= date, F.col(col)))
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
        he Dataframe to process
    column_format_map
        format of datetime string and associated list of column names to which it applies
    """
    for format, columns_list in column_format_map.items():
        for column_name in columns_list:
            if column_name in df.columns:
                df = df.withColumn(column_name, F.to_timestamp(F.col(column_name), format=format))
    return df


def apply_value_map_multiple_columns(df: DataFrame, column_map_dict: Mapping):
    """A wrapper around update_column_values_from_map function.

    Parameters
    ----------
    df
        The Dataframe to process
    column_map_dict
        A dictionary with column name (to edit) as Key and the value being another dictionary, whose
        keys are the values we want to replace by the corresponding value in the Key:Value pair

    """
    for col, map in column_map_dict.items():
        df = update_column_values_from_map(df, col, map)
    return df


def format_string_upper_and_clean(df: DataFrame, column_name_to_update: str) -> str:
    """
    Remove all instances of whitespace before and after a string field including all duplicate spaces
    along with dots (.) as well

    Parameters
    ----------
    df
        The Dataframe to process
    column_name_to_update
        The name of the column that will be formatted
    """
    df = df.withColumn(
        column_name_to_update,
        F.upper(F.ltrim(F.rtrim(F.regexp_replace(column_name_to_update, r"\s+", " ")))),
    )
    df = df.withColumn(
        column_name_to_update,
        F.when(
            F.substring(column_name_to_update, -1, 1) == ".",
            F.rtrim(F.col(column_name_to_update).substr(F.lit(1), F.length(column_name_to_update) - 1)),
        ).otherwise(F.col(column_name_to_update)),
    )

    return df


def rename_column_names(df: DataFrame, variable_name_map: dict) -> DataFrame:
    """
    Rename column names based on variable_name_map

    Parameters
    ----------
    df
        The Dataframe to process
    variable_name_map
        map of current column names to new names
    """
    cleaned_columns = [variable_name_map.get(old_column_name, old_column_name) for old_column_name in df.columns]
    return df.toDF(*cleaned_columns)


def assign_from_map(df: DataFrame, column_name_to_assign: str, reference_column: str, mapper: Mapping) -> DataFrame:
    """
    Assign column with values based on a dictionary map of reference_column.
    From households_aggregate_processes.xlsx, edit number 1.

    Parameters
    ----------
    df
        The Dataframe to process
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
        The Dataframe to process
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
    column_name_to_update: str,
    columns_to_sum: List[str],
    max_value: int,
):
    """
    Imputes column_name_to_update based on a sum of the columns_to_sum.
    If exceeds max, uses max_value. If all values are Null, sets outcome to Null.

    column_name_to_update must already exist on the df.

    Parameters
    ----------
    df
        The Dataframe to process
    column_name_to_update
        The column to impute (must already exist in the Dataframe)
    columns_to_sum
        List of column names to sum up
    max_value
        Max value which column_name_to_update can not exceed
    """
    df = df.withColumn(
        column_name_to_update,
        F.when(all_columns_null([column_name_to_update, *columns_to_sum]), None)
        .when(
            F.col(column_name_to_update).isNull(),
            F.least(F.lit(max_value), sum_within_row(columns_to_sum)),
        )
        .otherwise(F.col(column_name_to_update))
        .cast("integer"),
    )
    return df


def join_on_existing(df: DataFrame, df_to_join: DataFrame, on: List):
    """
    Join 2 dataframes on columns in 'on' list and
    override empty values in the left dataframe with values from the right
    dataframe.

    Parameters
    ----------
    df
      The Dataframe to process
    df_to_join
      the data frame you are left joining onto df, taking values from here when they don't exist in df
    on
      column/s on which to join the two dfs together

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


def convert_derived_columns_from_null_to_value(
    df: DataFrame, infection_symptom_dict: Dict[str, Tuple[str, str]], value: Any
):
    """
    Replaces Null values in columns with a new value conditionally depending on a value in another column, typically
    the precursor column although it doesn't have to be.

    Parameters
    ----------
    df : DataFrame
    infection_symptom_dict : Dict[str, Tuple[str, str]]
        A dictionary whose key is the column name or column prefix indicating which columns
        should have Null values converted to specified value. The value of the dictionary
        is a tuple with two elements. The first element is the column that the key column
        is derived from which will allow conditional replacement of Nulls based on the values
        in this column. The second element of the tuple is the value in the column for which
        Nulls will be replaced in the key column. E.g. if you want to replace all Nulls in
        column test_column when column original_column is 'Yes' you could supply:
        {test_column: (original_column, 'Yes')}
    value : Any
        The value to replace Nulls with on the target column/s.

    Returns
    -------
    df : DataFrame
        dataframe with replaced column values
    """
    for symptom_prefix, derive_col_val in infection_symptom_dict.items():
        derive_col, derive_val = derive_col_val
        if derive_col in df.columns:
            columns_to_replace = [col for col in df.columns if col.startswith(symptom_prefix)]
            df = df.select(
                *[col for col in df.columns if col not in columns_to_replace],
                *[
                    F.when(F.col(derive_col) == derive_val, F.coalesce(F.col(c), F.lit(value)))
                    .otherwise(F.col(c))
                    .alias(c)
                    for c in columns_to_replace
                ],
            )

    return df
