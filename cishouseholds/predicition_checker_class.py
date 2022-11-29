import imp
from pathlib import Path
from types import FunctionType

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from cishouseholds.pipeline.config import get_config


class PluginMeta(type):
    def __new__(cls, name, bases, dct):
        plugin_file_path = Path(dct["prediction_functions_file_path"])
        module = imp.load_source(plugin_file_path.stem, plugin_file_path)
        for name in dir(module):
            function = getattr(module, name)
            if isinstance(function, FunctionType):
                dct[function.__name__] = function
        return type.__new__(cls, name, bases, dct)


class PredictionChecker(PluginMeta):
    prediction_functions_file_path = get_config()["prediction_functions_file_path"]

    def __init__(self, base_df: DataFrame, compare_df: DataFrame, unique_id_column: str) -> None:
        base_df = base_df.select(*[c for c in base_df.columns if c in compare_df.columns])
        for col in base_df.columns:
            if col != unique_id_column:
                base_df = base_df.withColumnRenamed(col, f"{col}_ref")

        self.compare_df = compare_df
        self.base_df = base_df
        self.df = base_df.join(compare_df, on=unique_id_column, how="left")

    def get_predictions(self):
        return [
            method
            for method in dir(self)
            if not method.startswith("__")
            and callable(getattr(self, method))
            and method not in ["check_predictions", "get_predictions", "create_output"]
        ]

    def check_predictions(self):
        prediction_names = self.get_predictions()
        predictions = {}
        for prediction_name in prediction_names:
            prediction = getattr(self, prediction_name)
            predictions[prediction_name] = prediction()

        self.df = self.df.select(
            *[
                (F.sum(F.when(prediction, 1).otherwise(0)) / F.sum(F.lit(1)) * 100).alias(prediction_name)
                for prediction_name, prediction in predictions.items()
            ]
        )
        self.df = self.df.select(
            F.explode(
                F.array(
                    *[
                        F.struct(F.lit(c).alias("prediction_name"), F.col(c).alias("passed_percent"))
                        for c in prediction_names
                    ]
                )
            ).alias("kvs")
        )
        self.df = self.df.select("kvs.prediction_name", "kvs.passed_percent")
        return self.df
