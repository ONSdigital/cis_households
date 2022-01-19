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
        self.functions[function_name] = {"function": function_method, "error_message": error_message}

    def update_error_message(self, function_name, new_error_message):
        self.functions[function_name]["error_message"] = new_error_message

    def validate_column(self, operations):
        # operations : {"column_name": "method"(function or string)}
        for column_name, method in operations.items():
            check = self.functions[list(method.keys())[0]]
            self.execute_check(check["function"], check["error_message"], column_name, list(method.values())[0])

    def validate(self, operations):
        for method, params in operations.items():
            self.execute_check(self.functions[method]["function"], self.functions[method]["error_message"], **params)

    def execute_check(self, check, error_message, *params, **kwargs):
        self.dataframe = self.dataframe.withColumn(
            self.error_column,
            F.when(~check(*params, **kwargs), F.array_union(F.col(self.error_column), F.array(F.lit(error_message)))).otherwise(
                F.col(self.error_column)
            ),
        )

    @staticmethod
    def contains(column_name, contains):
        return F.col(column_name).rlike(contains)

    @staticmethod
    def isin(column_name, options):
        return F.col(column_name).isin(options)

    @staticmethod
    def between(column_name, range):
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
        return lb & ub

    # Non column wise functions
    @staticmethod
    def duplicated(column_list):
        window = Window.partitionBy(*column_list)
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False)
