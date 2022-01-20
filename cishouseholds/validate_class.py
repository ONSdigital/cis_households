from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import DataFrame


class SparkValidate:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe
        self.error_column = "error"
        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())

        self.functions = {
            "contains": {"function": self.contains, "error_message": "{} should contain '{}'."},
            "isin": {"function": self.isin, "error_message": "{}, the row is '{}'"},
            "duplicated": {"function": self.duplicated, "error_message": "{} should be unique."},
            "between": {"function": self.between, "error_message": "{} should be in between {} and {}."},
        }

    def new_function(self, function_name, function_method, error_message="default error"):
        self.functions[function_name] = {"function": function_method, "error_message": error_message}

    def set_error_message(self, function_name, new_error_message):
        self.functions[function_name]["error_message"] = new_error_message

    def validate_column(self, operations):
        # operations : {"column_name": "method"(function or string)}

        for column_name, method in operations.items():
            check = self.functions[list(method.keys())[0]]
            self.execute_check(check["function"], check["error_message"], column_name, list(method.values())[0])

    def validate(self, operations):
        for method, params in operations.items():
            self.execute_check(self.functions[method]["function"], self.functions[method]["error_message"], **params)

    def validate_udl(self, logic, error_message):
        self.execute_check(logic, error_message)

    def execute_check(self, check, error_message, *params, **kwargs):
        if callable(check):
            check, error_message = check(error_message, *params, **kwargs)

        self.dataframe = self.dataframe.withColumn(
            self.error_column,
            F.when(~check, F.array_union(F.col(self.error_column), F.array(F.lit(error_message)))).otherwise(
                F.col(self.error_column)
            ),
        )

    @staticmethod
    def contains(error_message, column_name, contains):
        error_message = error_message.format(column_name, contains)
        return F.col(column_name).rlike(contains), error_message

    @staticmethod
    def isin(error_message, column_name, options):
        error_message = error_message.format(column_name, ", ".join(options))
        return F.col(column_name).isin(options), error_message

    @staticmethod
    def between(error_message, column_name, range):
        lb = (
            (F.col(column_name) >= range["lower_bound"]["value"])
            if range["lower_bound"]["inclusive"]
            else (F.col(column_name) > range["lower_bound"]["value"])
        )
        ub = (
            (F.col(column_name) <= range["upper_bound"]["value"])
            if range["upper_bound"]["inclusive"]
            else (F.col(column_name) < range["upper_bound"]["value"])
        )
        error_message = error_message.format(column_name, *range)
        return lb & ub, error_message

    # Non column wise functions
    @staticmethod
    def duplicated(error_message, column_list):
        window = Window.partitionBy(*column_list)
        error_message = error_message.format(", ".join(column_list))
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False), error_message
