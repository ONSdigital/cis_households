from typing import Any
from typing import Callable
from typing import Dict
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import DataFrame


class SparkValidate:
    """
    Class that validates dataframes to the methods specified (see below in methods).
    """

    def __init__(self, dataframe: DataFrame, error_column_name: str) -> None:
        """
        Initialisation of SparkValidate with the compulsory input parameters.

        Parameters
        ----------
        dataframe : DataFrame
            dataframe object in which validation will be carried over
        error_column_name : str
            Column in which the validation messages will be appended in a list
        """
        self.dataframe = dataframe
        self.error_column = error_column_name

        if error_column_name in dataframe.columns:
            raise ValueError(f"A column by this name ({error_column_name}) already exists please choose another name")

        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())
        self.error_column_list: List[Any] = []

        self.functions = {
            "contains": {"function": self.contains, "error_message": "{} should contain '{}'"},
            "starts_with": {"function": self.contains, "error_message": "{} should start with '{}'"},
            "matches": {"function": self.contains, "error_message": "{} should match '{}'"},
            "isin": {"function": self.isin, "error_message": "{} should be in [{}]"},
            "duplicated": {"function": self.duplicated, "error_message": "{} should be unique"},
            "between": {"function": self.between, "error_message": "{} should be between {}{} and {}{}"},
            "null": {"function": self.not_null, "error_message": "{} should not be null"},
            "valid_vaccination": {"function": self.valid_vaccination, "error_message": "invalid vaccination"},
            "valid_file_date": {
                "function": self.check_valid_file_date,
                "error_message": "the date in {} should be before the date in {} plus two days when both {} and {} are null",  # noqa:E501
            },
            "check_all_null_given_condition": {
                "function": self.check_all_null_given_condition,
                "error_message": "{} should all be null given condition {}",
            },
        }

    # NOTE: The following method is not referenced anywhere
    def new_function(self, function_name: str, function_method, error_message: str = "default error"):
        """
        This function will add a new named method to the class which can be called from the dictionary of
        method objects.
        Parameters
        ----------
        function_name
        function_method
            function object defined by user
        error_message
        """
        self.functions[function_name] = {"function": function_method, "error_message": error_message}

    # NOTE: The following method is not referenced anywhere
    def set_error_message(self, function_name: str, new_error_message: str):
        """
        Replaces the error message for the specified function.
        Parameters
        ----------
        function_name
        new_error_message
        """
        self.functions[function_name]["error_message"] = new_error_message

    def produce_error_column(self):
        """
        Creates the error column in which all the errors will be listed in an array.
        NOTE: error_column name is specified in init()
        """
        self.dataframe = self.dataframe.withColumn(
            self.error_column, F.concat(F.col(self.error_column), F.array([col for col in self.error_column_list]))
        )
        self.dataframe = self.dataframe.withColumn(
            self.error_column, F.expr(f"filter({self.error_column}, x -> x is not null)")
        )
        self.error_column_list = []

    def filter(self, return_failed: bool, any: bool, selected_errors: List = []):
        """
        Returns Dataframe with validation column filtered by the errors wanted.

        Parameters
        ----------
        return_failed : bool
            returns a subset of the failed rows
        any : bool
            if True, will return any conditions not met and if false will return all
        selected_errors : List
            list of errors that denote a failed row
        """
        if len(self.error_column_list) != 0:
            self.produce_error_column()
        if len(selected_errors) == 0 or any:
            min_size = 1
        else:
            min_size = len(selected_errors)
        failed_df = self.dataframe.filter(
            F.size(F.array_intersect(F.col(self.error_column), F.array([F.lit(error) for error in selected_errors])))
            >= min_size
        )
        passed_df = self.dataframe.filter(
            F.size(F.array_intersect(F.col(self.error_column), F.array([F.lit(error) for error in selected_errors])))
            < min_size
        )
        self.passed_df = passed_df
        self.failed_df = failed_df
        if return_failed:
            return passed_df, failed_df
        return passed_df

    def validate_column(self, operations: Dict[str, Dict[str, Callable]]):
        """
        Executes validation by given number of columns and logic validation.

        Parameters
        ----------
        operations : dict
            dictionary with key to be column name, and value to be the function logic.
        """
        # operations : {"column_name": "method"(function or string)}
        for column_name, method in operations.items():
            if column_name not in self.dataframe.columns:
                print(
                    f"    - Validation rule references {column_name} column and it is not in the dataframe."
                )  # functional
            else:
                check = self.functions[list(method.keys())[0]]
                if "subset" in method:
                    self.execute_check(
                        check["function"],
                        check["error_message"],
                        column_name,
                        list(method.values())[0],
                        subset=method["subset"],
                    )
                else:
                    self.execute_check(check["function"], check["error_message"], column_name, list(method.values())[0])

    def validate_all_columns_in_df(self, operations: dict):
        """
        Executes validation by given logic validation to all columns.

        Parameters
        ----------
        operations : dict
            should be a dictionary with key being the name of the check and value should be another
            dictionary or list depending on the check.
        """
        for method, params in operations.items():
            if type(params) != list:
                params = [params]
            for p in params:
                self.execute_check(self.functions[method]["function"], self.functions[method]["error_message"], **p)

    def validate_user_defined_logic(self, logic: Any, error_message: str, columns: List[str]):
        """
        Specific user defined validation by given logic on provided list of columns.

        Parameters
        ----------
        logic
            The pyspark logic to be applied. This should return a True or False outcome to denote
            whether the check has passed or failed, respectively.
        error_message : str
            String pattern to be added to list of errors if the check fails
        columns : List[str]
            List of columns used in the function
        """
        missing = list(set(columns).difference(set(self.dataframe.columns)))
        if len(missing) == 0:
            self.execute_check(logic, error_message)
        else:
            print(
                "    - Falied to run check as required " + ",".join(missing) + " missing from dataframe"
            )  # functional

    def execute_check(self, check, error_message: object, *params, subset=None, **kwargs):
        """
        Validates check provided by a given instance of SparkValidate Class.
        Can be used separately but this method will be used as part of validate_column,
        validate_all_columns or validate_user_defined_logic.

        Parameters
        ----------
        check
            Validation logic to be checked, where outcome True if pass and False if condition not met.
        error_message : object
            String pattern to be added to list of errors if the check fails
        *params
            ??
        subset
            Handles row filtering logic. Default is None
        **kwargs
            ??
        """
        if callable(check):
            check, error_message = check(error_message, *params, **kwargs)
        if subset is not None:
            self.error_column_list.append(F.when(~check & subset, F.lit(error_message)).otherwise(None))
        else:
            self.error_column_list.append(F.when(~check, F.lit(error_message)).otherwise(None))

    def count_complete_duplicates(self, duplicate_count_column_name: str):
        """
        Finds duplicated values by a given column name.

        Parameters
        ----------
        duplicate_count_column_name : str
           Name of column to be checked.
        """
        self.dataframe = (
            self.dataframe.groupBy(*self.dataframe.columns)
            .count()
            .withColumnRenamed("count", duplicate_count_column_name)
        )

    @staticmethod
    def not_null(error_message: str, check_columns):  # works in validate and validate_column
        """
        Check any column contains nulls.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        check_columns : str or List[str]
           The column(s) to be checked.
        """
        error_message = error_message.format(", ".join(check_columns))
        if type(check_columns) == str:
            check_columns = [check_columns]
        return (
            F.when(sum([F.isnull(F.col(col)).cast("integer") for col in check_columns]) > 0, False).otherwise(True),
            error_message,
        )

    @staticmethod
    def contains(error_message: str, column_name: str, pattern):
        """
        Finds what columns have a specific pattern.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        check_columns : str
           The column to be checked.
        pattern
           Pattent to check against
        """
        error_message = error_message.format(column_name, pattern)
        return F.col(column_name).rlike(pattern), error_message

    @staticmethod
    def starts_with(error_message: str, column_name: str, pattern: str):
        """
        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        column_name : str
           Name of column to check.
        pattern
            Regex pattern to check against
        """
        error_message = error_message.format(column_name, pattern)
        return F.col(column_name).startswith(pattern), error_message

    @staticmethod
    def isin(error_message: str, column_name: str, options: List):
        """
        Checks that the content of a column is in a list of options.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        column_name : str
           Column to be checked.
        options : List
           List of options to check against.
        """
        error_message = error_message.format(column_name, "'" + "', '".join(options) + "'")
        return F.col(column_name).isin(options), error_message

    @staticmethod
    def between(error_message: str, column_name: str, range_set: List):
        """
        Check that column_name is between range_set.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        column_name : str
           Column to be checked.
        range_set : List
            List composed of low and high bounds.
        """
        if type(range_set) != list:
            range_set = [range_set]
        bools = []
        for range in range_set:
            lower_bound = (
                (F.col(column_name) >= range["lower_bound"]["value"])
                if range["lower_bound"]["inclusive"]
                else (F.col(column_name) > range["lower_bound"]["value"])
            )
            upper_bound = (
                (F.col(column_name) <= range["upper_bound"]["value"])
                if range["upper_bound"]["inclusive"]
                else (F.col(column_name) < range["upper_bound"]["value"])
            )
            error_message = error_message.format(
                column_name,
                range["lower_bound"]["value"],
                " (inclusive)" if range["lower_bound"]["inclusive"] else "",
                range["upper_bound"]["value"],
                " (inclusive)" if range["upper_bound"]["inclusive"] else "",
            )
            if "allow_none" in range and range["allow_none"] is True:
                bools.append((lower_bound & upper_bound) | (F.col(column_name).isNull()))
            else:
                bools.append(lower_bound & upper_bound)
        return F.array_contains(F.array(*bools), True), error_message

    # Non column wise functions
    @staticmethod
    def duplicated(error_message: str, check_columns: List[str]):
        """
        Finds duplicated values by given column and can specify specific error message.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        check_columns : List[str]
            Columns to be checked.
        """
        window = Window.partitionBy(*check_columns)
        error_message = error_message.format(", ".join(check_columns))
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False), error_message

    @staticmethod
    def valid_vaccination(error_message: str, survey_response_type_column: str, check_columns: List[str]):
        """
        Works out valid vaccination by finding "First Visit" or None

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        survey_response_type_column : str
            ???
        check_columns : List[str]
            List of columns to check.
        """
        return (
            (F.col(survey_response_type_column) != "First Visit") | (~F.array_contains(F.array(*check_columns), None)),
            error_message,
        )

    @staticmethod
    def check_all_null_given_condition(error_message: str, condition: Any, null_columns: List[str]):
        """
        Check all columns in list are null if they meet a specific logic condition.\

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        condition :
           The logical condition of relevance.
        null_columns : List[str]
           The columns expected to be null given the logical condition.
        """
        error_message = error_message.format(", ".join(null_columns), str(condition))
        return (
            F.when(
                condition
                & (
                    F.size(
                        F.array_remove(
                            F.array([F.when(F.col(col).isNull(), 1).otherwise(0) for col in null_columns]), 0
                        )
                    )
                    == len(null_columns)
                ),
                True,
            ).otherwise(False),
            error_message,
        )

    @staticmethod
    def check_valid_file_date(
        error_message: str,
        visit_datetime_column: str,
        file_date_column: str,
        swab_barcode_column: str,
        blood_barcode_column: str,
    ):
        """
        Ensure that the file date column is at least 2 days earlier than visit_datetime
        and swab/blood barcodes are null.
        # TODO: Check logic and confirm this description correct.

        Parameters
        ----------
        error_message : str
            String pattern to be added to list of errors if the check fails
        visit_datetime_column : str
           Header of visit datetime column.
        file_date_column
        swab_barcode_column
        blood_barcode_column
        """
        error_message = error_message.format(
            visit_datetime_column, file_date_column, swab_barcode_column, blood_barcode_column
        )
        return (
            F.when(
                (
                    (F.col(visit_datetime_column) > ((F.col(file_date_column) + F.expr("INTERVAL 2 DAYS"))))
                    & F.col(swab_barcode_column).isNull()
                    & F.col(blood_barcode_column).isNull()
                ),
                False,
            ).otherwise(True),
            error_message,
        )
