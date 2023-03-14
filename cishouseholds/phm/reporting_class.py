from _datetime import datetime
from io import BytesIO

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.hdfs_utils import write_string_to_file


class Report:
    def __int__(self):
        """"""
        self.sheets = []
        self.output = BytesIO()

    def add_sheet(self, df, sheet_name):
        """"""
        with pd.ExcelWriter(self.output) as writer:
            df.toPandas().to_excel(writer, sheet_name=sheet_name, index=False)

    def write_excel_output(self, output_directory):

        write_string_to_file(
            self.output.getbuffer(),
            f"{output_directory}/phm_report_output_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx",
        )

    @staticmethod
    def add_start_end_delta_columns(
        df: DataFrame,
        start_reference_date_column: str,
        reference_date_column: str,
        start_column_name: str = "START",
        end_column_name: str = "END",
        days_range: int = 28,
    ):
        """
        Create columns for the start and end dates of sequential windows of a given width

        Parameters
        ----------
        df
        reference_date_column
            A column containing dates which dictate the start of windows
        start_column_ name
        end_column_name
        days_range
            the number of days a window should span including day 0 and end date
        """
        window = Window.orderBy(start_reference_date_column)
        win_diff = F.floor(
            F.datediff(F.col(reference_date_column), F.first(start_reference_date_column).over(window)) / days_range
        )

        df = df.withColumn("TO_ADD_1", win_diff * (days_range))
        df = df.withColumn("TO_ADD_2", (win_diff * (days_range)) + days_range - 1)
        df = df.withColumn("FIRST", F.first(start_reference_date_column).over(window))

        df = df.withColumn(start_column_name, F.expr("date_add(FIRST, TO_ADD_1)"))
        df = df.withColumn(end_column_name, F.expr("date_add(FIRST, TO_ADD_2)"))
        return df.drop("TO_ADD_1", "TO_ADD_2", "FIRST")

    @staticmethod
    def completion_table_process(
        df: DataFrame,
        reference_date_column: str,
        window_start_column: str,
        window_end_column: str,
        full_column_name: str,
        partial_column_name: str,
        window_range: int = None,
    ):
        """"""
        # select appropriate columns
        df = df.select(
            window_start_column,
            window_end_column,
            reference_date_column,
            "daily_" + full_column_name,
            "daily_" + partial_column_name,
        ).distinct()
        df = df.withColumn("DIFF", F.datediff(F.col(reference_date_column), F.col(window_start_column)) + 1)

        # find the max difference
        if not window_range:
            window_range = df.select("DIFF").rdd.max()[0]

        # create completion rate columns
        dff = (
            df.groupBy(window_start_column, window_end_column)
            .pivot("DIFF")
            .agg(
                F.first("daily_" + full_column_name).alias(full_column_name),
                F.first("daily_" + partial_column_name).alias(partial_column_name),
            )
            .fillna(0)
        )
        partial_columns = [c for c in dff.columns if c.endswith(partial_column_name)]
        full_columns = [c for c in dff.columns if c.endswith(full_column_name)]

        partial_df = dff.select(window_start_column, window_end_column, *partial_columns)
        partial_df = partial_df.withColumn("total", sum([F.col(c) for c in partial_columns]))

        for col in partial_columns:
            partial_df = partial_df.withColumnRenamed(col, col.split("_")[0])

        full_df = dff.select(window_start_column, window_end_column, *full_columns)
        full_df = full_df.withColumn("total", sum([F.col(c) for c in full_columns]))
        for col in full_columns:
            full_df = full_df.withColumnRenamed(col, col.split("_")[0])

        for i in range(1, window_range + 1):  # type: ignore
            if str(i) not in partial_df.columns:
                partial_df = partial_df.withColumn(str(i), F.lit(0).cast("double"))
            if str(i) not in full_df.columns:
                full_df = full_df.withColumn(str(i), F.lit(0).cast("double"))

        # rearrange columns
        columns = (
            [window_start_column, window_end_column]
            + ["day_" + str(i) for i in list(range(1, window_range + 1))]  # type: ignore
            + ["total"]
        )
        for col in [str(i) for i in range(1, window_range + 1)]:  # type: ignore
            partial_df = partial_df.withColumnRenamed(col, "day_" + col)
            full_df = full_df.withColumnRenamed(col, "day_" + col)
        return partial_df.select(*columns), full_df.select(*columns)

    def create_completion_table_days(
        self,
        df: DataFrame,
        participant_id_column: str,
        reference_date_column: str,
        window_start_column: str,
        window_end_column: str,
        window_status_column: str,
        window_range: int = None,
    ):
        """"""
        window_a = Window.partitionBy(window_start_column, window_end_column, reference_date_column)
        window_b = Window.partitionBy(window_start_column, window_end_column)

        df = df.select(
            participant_id_column, window_start_column, window_end_column, reference_date_column, window_status_column
        ).distinct()

        df = df.withColumn(
            "daily_full_completion_count",
            F.sum(F.when(F.col(window_status_column) == "Submitted", 1).otherwise(0)).over(window_a),
        )
        df = df.withColumn(
            "daily_full_completion_rate",
            F.col("daily_full_completion_count") / F.count(participant_id_column).over(window_b),
        )

        df = df.withColumn(
            "daily_partial_completion_count",
            F.sum(F.when(F.col(window_status_column) == "Completed", 1).otherwise(0)).over(window_a),
        )
        df = df.withColumn(
            "daily_partial_completion_rate",
            F.col("daily_partial_completion_count") / F.count(participant_id_column).over(window_b),
        )

        partial_df_count, full_df_count = self.completion_table_process(
            df=df,
            reference_date_column=reference_date_column,
            window_start_column=window_start_column,
            window_end_column=window_end_column,
            full_column_name="full_completion_count",
            partial_column_name="partial_completion_count",
            window_range=window_range,
        )

        partial_df_rate, full_df_rate = self.completion_table_process(
            df=df,
            reference_date_column=reference_date_column,
            window_start_column=window_start_column,
            window_end_column=window_end_column,
            full_column_name="full_completion_rate",
            partial_column_name="partial_completion_rate",
            window_range=window_range,
        )

        self.add_sheet(partial_df_count, "Partial completion counts")
        self.add_sheet(full_df_count, "Full completion counts")
        self.add_sheet(partial_df_rate, "Partial completion rates")
        self.add_sheet(full_df_rate, "Full completion rates")

        return partial_df_rate, full_df_rate

    def create_completion_table_set_range(
        self,
        df: DataFrame,
        participant_id_column: str,
        reference_date_column: str,
        window_start_column: str,
        window_end_column: str,
        window_status_column: str,
        window_range: int = 28,
    ):
        """"""
        df = self.add_start_end_delta_columns(
            df=df,
            start_column_name="START",
            end_column_name="END",
            days_range=window_range,
            start_reference_date_column=window_start_column,
            reference_date_column=reference_date_column,
        )
        partial_df, full_df = self.create_completion_table_days(
            df=df,
            participant_id_column=participant_id_column,
            window_start_column="START",
            window_end_column="END",
            window_status_column=window_status_column,
            reference_date_column=reference_date_column,
            window_range=window_range,
        )
        partial_df = partial_df.withColumn("date_range", F.concat_ws("-", "START", "END")).drop("START", "END")
        full_df = full_df.withColumn("date_range", F.concat_ws("-", "START", "END")).drop("START", "END")
        return partial_df, full_df
