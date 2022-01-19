from curses import window
import imp
from pyspark.sql.functions import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

class SparkValidate:
    def __init__(self, dataframe:DataFrame) -> None:
        self.dataframe = dataframe
        self.error_column = "error"
        self.dataframe = self.dataframe.withColumn(self.error_column, F.array())

        self.functions = {
            "contains":{"function":self.contains, "error_message":"error!"},
            "isin":{"function":self.isin, "error_message":"error!"}
        }

    def validate(self, operations):
        # operations : {"column_name": "method"(function or string)}
        for column_name, method in operations.items():
            check = self.functions[list(method.keys())[0]]
            self.execute_check(check["function"],check["error_message"],column_name,list(method.values())[0])


        # self.dataframe = (reduce(
        #     lambda df, col_name: df.withColumn(col_name, lower(col(col_name))),
        #     source_df.columns,
        #     source_df
        # ))
    def execute_check(self, check, error_message, *params):
        self.dataframe = self.dataframe.withColumn(self.error_column, F.when(~check(*params),F.array_union(F.col(self.error_column),F.array(F.lit(error_message)))).otherwise(F.col(self.error_column)))

    def contains(self, column_name,contains):
       return F.col(column_name).rlike(contains)

    def isin(self, column_name, options):
        return F.col(column_name).isin(options)

    def between(self, column_name, range):
        return (F.col(column_name) >= range["lower_bound"]) & (F.col(column_name) <= range["upper_bound"])

    def duplicated(self, column_list):
        window = Window.partitionBy(*column_list)
        return F.when(F.sum(1).over(window) == 1, True).otherwise(False)
