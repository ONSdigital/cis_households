from functools import reduce
from operator import add
from operator import and_
from operator import or_
from typing import Any
from typing import List

import pyspark.sql.functions as F


def set_date_component(date_column: str, date_component: str, set_to: Any):
    regex_lookup = {"year": r"^\d{4,4}(?=-)", "month": r"(?<=-)\d{2,2}(?=-)", "day": r"(?<=-)\d{2,2}$"}
    datetime = F.date_format(date_column, "yyyy-MM-dd HH:mm:ss")
    split_datetime = F.split(datetime, " ")
    date_lookup = {key: F.regexp_extract(split_datetime.getItem(0), val, 0) for key, val in regex_lookup.items()}
    date_lookup[date_component] = F.lit(set_to)
    new_date = F.concat_ws("-", *list(date_lookup.values()))
    return F.to_timestamp(F.concat_ws(" ", new_date, split_datetime.getItem(1)))


def any_column_not_null(column_list: List[str]):
    "Expression that evaluates true if any column is not null."
    return reduce(or_, [F.col(column).isNotNull() for column in column_list])


def all_columns_not_null(column_list: List[str]):
    "Expression that evaluates true if all columns are null."
    return reduce(and_, [F.col(column).isNotNull() for column in column_list])


def array_contains_any(array_column: str, values: List):
    "check if array column contains any value in list"
    return reduce(or_, [F.array_contains(array_column, val) for val in values])


def all_columns_null(column_list: List[str]):
    "Expression that evaluates true if all columns are null."
    return reduce(and_, [F.col(column).isNull() for column in column_list])


def all_columns_values_in_list(column_list: List[str], values):
    """Expression that evalates true if all columns equal a certain value."""
    if not isinstance(values, list):
        values = [values]
    return reduce(and_, [F.col(column).isin(values) for column in column_list])


def any_column_null(column_list: List[str]):
    "Expression that evaluates true if any column is null."
    return reduce(or_, [F.col(column).isNull() for column in column_list])


def any_column_equal_value(column_list: List[str], val):
    "Expression that evaluates true if any column matches val"
    return reduce(or_, [F.col(column).eqNullSafe(val) for column in column_list])


def first_sorted_val_row_wise(column_list: List[str]):
    "Expression to return the first sorted value row-wise"
    return F.array_sort(F.array(column_list)).getItem(0)


def last_sorted_val_row_wise(column_list: List[str]):
    "Expression to return the first sorted value row-wise"
    return F.array_sort(F.array(column_list)).getItem(len(column_list) - 1)


def all_equal(column_list: List[str], equal_to: Any):
    "Expression that evaluates true if all columns are equal to the specified value."
    return reduce(and_, [F.col(column).eqNullSafe(F.lit(equal_to)) for column in column_list])


def all_equal_or_Null(column_list: List[str], equal_to: Any):
    "Expression that evaluates true if all columns are equal to the specified value OR Null."
    return reduce(
        and_, [(F.col(column).isNull() | F.col(column).eqNullSafe(F.lit(equal_to))) for column in column_list]
    )


def sum_within_row(column_list: List[str]):
    """
    Sum of values from one or more columns within a row.
    N.B. Null values are treated as 0. If is values are Null, sum will be 0.
    """
    return reduce(add, [F.coalesce(F.col(column), F.lit(0)) for column in column_list])


def count_occurrence_in_row(column_list: List[str], val_to_count: Any):
    """
    Count occurrence of a value in a row
    """
    return reduce(add, [F.when(F.col(column).eqNullSafe(val_to_count), 1).otherwise(0) for column in column_list])


def any_column_matches_regex(column_list: List[str], regex_pattern: str):
    """
    Expression that evaluates to true if any column matches the given RegEx pattern. Null values in a column
    are replaced with 0-length strings - this prevents the result from being evaluated as null when one or
    more columns contain a null value.
    """
    return reduce(or_, [F.coalesce(F.col(column), F.lit("")).rlike(regex_pattern) for column in column_list])
