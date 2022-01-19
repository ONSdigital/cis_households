import imp
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import DataFrame


class SparkValidate:
    def __init__(self, dataframe: DataFrame) -> None:
        self.dataframe = dataframe
        self.error_column = "error"
        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())

        self.functions = {
            "contains": {"function": self.contains, "error_message": "not_contained"},
            "isin": {"function": self.isin, "error_message": "isnt_in"},
            "duplicated": {"function": self.duplicated, "error_message": "duplication"},
            "between": {"function": self.between, "error_message": "not_between"},
        }

    def new_function(self, function_name, function_method, error_message="default error"):
        self.functions[function_name] = {"function":function_method, "error_message":error_message}

    def update_error_message(self, function_name, new_error_message):
        self.functions[function_name]["error_message"] = new_error_message

    def validate_column(self, operations):
        # operations : {"column_name": "method"(function or string)}
        for column_name, method in operations.items():
            check = self.functions[list(method.keys())[0]]
            self.execute_check(check["function"], check["error_message"], column_name, list(method.values())[0])

        operations = (reduce(
            lambda df, col_name: self.execute_check(check["function"],check["error_message"],column_name,list(method.values())[0]),
            self.dataframe.columns,
            self.dataframe
        ))

    def validate(self, operations):
        for method, param in operations.items():
            self.execute_check(self.functions[method]["function"], self.functions[method]["error_message"], param)

    def execute_check(self, check, error_message, *params):
        self.dataframe = self.dataframe.withColumn(
            self.error_column,
            F.when(~check(*params), F.array_union(F.col(self.error_column), F.array(F.lit(error_message)))).otherwise(
                F.col(self.error_column)
            ),
        )

    def contains(self, column_name, contains):
        return F.col(column_name).rlike(contains)

    def isin(self, column_name, options):
        return F.col(column_name).isin(options)

    def between(self, column_name, range):

        lb = (
            (F.col(column_name) >= range["lower_bound"])
            if range["lower_bound"]["inclusive"]
            else (F.col(column_name) > range["lower_bound"])
        )

        ub = (
            (F.col(column_name) <= range["upper_bound"])
            if range["upper_bound"]["inclusive"]
            else (F.col(column_name) < range["upper_bound"])
        )

        return lb & ub


    # Non column wise functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def duplicated(self, column_list):
        window = Window.partitionBy(*column_list)
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False)
<<<<<<< HEAD

    
=======
>>>>>>> 0eb03a4e50834cab811f81ec09634074f8290810
