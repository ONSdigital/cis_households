from functools import reduce
from operator import add
from operator import and_
from operator import or_
from typing import Any

import pyspark.sql.functions as F


def any_column_not_null(column_list: list):
    "Expression that evaluates true if any column is not null."
    return reduce(or_, [F.col(column).isNotNull() for column in column_list])


def any_column_null(column_list: list):
    "Expression that evaluates true if any column is null."
    return reduce(and_, [F.col(column).isNull() for column in column_list])


def all_equal(column_list: list, equal_to: Any):
    "Expression that evaluates true if all columns are equal to the specified value."
    return reduce(and_, [F.col(column).eqNullSafe(F.lit(equal_to)) for column in column_list])


def sum_within_row(column_list: list):
    """
    Sum of values from one or more columns within a row.
    N.B. Null values are treated as 0. If is values are Null, sum will be 0.
    """
    return reduce(add, [F.coalesce(F.col(column), F.lit(0)) for column in column_list])
