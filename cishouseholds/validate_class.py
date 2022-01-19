import imp
from pyspark.sql.functions import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
from functools import reduce

class SparkValidate:
    def __init__(self, dataframe:DataFrame) -> None:
        self.dataframe = dataframe
        self.error_column = "error"
        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())

        self.functions = {
            "contains":{"function":self.contains, "error_message":"error!"},
            "isin":{"function":self.isin, "error_message":"error!"},
            "duplicated":{"function":self.duplicated, "error_message":"error!"},
        }

    def new_function(self, function_name, function_method, error_message="default error"):
        self.functions[function_name] = {"function":function_method, "error_message":error_message}

    def update_error_message(self, function_name, new_error_message):
        self.functions[function_name]["error_message"] = new_error_message

    def validate_column(self, operations):
        # operations : {"column_name": "method"(function or string)}
        for column_name, method in operations.items():
            check = self.functions[list(method.keys())[0]]
            self.execute_check(check["function"],check["error_message"],column_name,list(method.values())[0])


        operations = (reduce(
            lambda df, col_name: self.execute_check(check["function"],check["error_message"],column_name,list(method.values())[0]),
            self.dataframe.columns,
            self.dataframe
        ))

    def validate(self, operations):
        for method, param in operations.items(): 
            self.execute_check(self.functions[method]["function"], self.functions[method]["error_message"], param)

    def execute_check(self, check, error_message, *params):
        self.dataframe = self.dataframe.withColumn(self.error_column, F.when(~check(*params),F.array_union(F.col(self.error_column),F.array(F.lit(error_message)))).otherwise(F.col(self.error_column)))

    def contains(self, column_name,contains):
       return F.col(column_name).rlike(contains)

    def isin(self, column_name, options):
        return F.col(column_name).isin(options)

    def between(self, column_name, range):
        return (F.col(column_name) >= range["lower_bound"]) & (F.col(column_name) <= range["upper_bound"])


    # Non column wise functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def duplicated(self, column_list):
        window = Window.partitionBy(*column_list)
        return F.when(F.sum(F.lit(1)).over(window) == 1, True).otherwise(False)

    
