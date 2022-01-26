import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import DataFrame


class SparkValidate:
    def __init__(self, dataframe: DataFrame, error_column_name: str) -> None:
        self.dataframe = dataframe
        self.error_column = error_column_name
        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())

        self.functions = {
            "contains": {"function": self.contains, "error_message": "{} should contain '{}'"},
            "isin": {"function": self.isin, "error_message": "{} the row is '{}'"},
            "duplicated": {"function": self.duplicated, "error_message": "{} should be unique"},
            "between": {"function": self.between, "error_message": "{} should be in between {} and {}"},
            "null": {"function": self.not_null, "error_message": "{} should not be null"},
            "valid_vaccination": {"function": self.valid_vaccination, "error_message": "invalid vaccination"},
        }

    def new_function(self, function_name, function_method, error_message="default error"):
        self.functions[function_name] = {"function": function_method, "error_message": error_message}

    def set_error_message(self, function_name, new_error_message):
        self.functions[function_name]["error_message"] = new_error_message

    def report(self, selected_errors, all=False):
        min_size = 1
        if all:
            min_size = len(selected_errors)
        passed_df = self.dataframe.filter(
            F.size(F.array_intersect(F.col(self.error_column), F.array([F.lit(error) for error in selected_errors])))
            >= min_size
        )
        failed_df = self.dataframe.filter(
            F.size(F.array_intersect(F.col(self.error_column), F.array([F.lit(error) for error in selected_errors])))
            < min_size
        )
        return passed_df, failed_df

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
    def not_null(error_message, check_columns):  # works in validate and validate_column
        error_message = error_message.format(", ".join(check_columns))
        return (
            F.when(sum([F.isnull(F.col(col)).cast("integer") for col in check_columns]) > 0, False).otherwise(True),
            error_message,
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
    def duplicated(error_message, check_columns):
        window = Window.partitionBy(*check_columns)
        error_message = error_message.format(", ".join(check_columns))
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False), error_message

    @staticmethod
    def valid_vaccination(error_message, visit_type_column, check_columns):
        return (F.col(visit_type_column) != "First Visit") | (
            ~F.array_contains(F.array(*check_columns), None)
        ), error_message

    @staticmethod
    def validate_relationship_between_socialcare_columns(
        work_social_care_column,
        work_care_nursing_home_column,
        work_direct_contact_column,
        value_yes="yes",
        value_no="no",
    ):
        return F.when(
            (F.col(work_social_care_column) == value_yes)
            & (F.col(work_care_nursing_home_column) | F.col(work_direct_contact_column))
        )
