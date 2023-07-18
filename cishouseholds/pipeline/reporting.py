from datetime import datetime
from io import BytesIO
from typing import Dict
from typing import List
from typing import Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.edit import update_column_values_from_map
from cishouseholds.expressions import all_columns_not_null
from cishouseholds.hdfs_utils import write_string_to_file
from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_run_id
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.validate import validate_processed_files


def count_variable_option(df: DataFrame, column_inv: str, column_value: str):
    """
    Counts occurence of a specific value in a column

    Parameters
    --------
    df
        Dataframe
    column_inv
        column you want the value count from
    column_value
        value you want counting in the specified column

    """

    df_filt = df.withColumn("col_value", F.when(F.col(column_inv) == column_value, F.lit(1)).otherwise(0))
    sum_value = df.count() - df_filt.filter(df_filt["col_value"] == 0).count()

    count_data = [
        # fmt:off
        (column_inv, column_value, sum_value)
        # fmt:on
    ]

    schema = """
            column_name string,
            column_value string,
            count integer
            """

    output_df = get_or_create_spark_session().createDataFrame(data=count_data, schema=schema)
    return output_df


def dfs_to_bytes_excel(sheet_df_map: Dict[str, DataFrame]) -> BytesIO:
    """
    Convert a dictionary of Spark DataFrames into an Excel Object.

    Parameters
    ----------
    sheet_df_map
        A dictionary of Spark DataFrames - keys in this dictionary become
        names of the sheets in Excel & the values in this dictionary become
        the content in the respective sheets
    """
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet, df in sheet_df_map.items():
            df.toPandas().to_excel(writer, sheet_name=sheet, index=False)
    return output


def generate_error_table(table_name: str, error_priority_map: dict) -> DataFrame:
    """
    Generates tables of errors and their respective counts present in
    current and previous pipeline run ordered by a custom priorty ranking
    set in pipeline config.

    Parameters
    ----------
    table_name
        Name of a hdfs table of survey responses passing/failing validation checks
    error_priority_map
        Error priority dictionary
    """
    df = extract_from_table(table_name)
    df_new = df.filter(F.col("run_id") == get_run_id()).groupBy("validation_check_failures").count()
    df_previous = df.filter(F.col("run_id") == (get_run_id() - 1)).groupBy("validation_check_failures").count()
    df = (
        df_previous.withColumnRenamed("count", "count_previous")
        .withColumnRenamed("run_id", "run_id_previous")
        .join(
            df_new.withColumnRenamed("count", "count_current").withColumnRenamed("run_id", "run_id_current"),
            on="validation_check_failures",
            how="fullouter",
        )
    )
    df = df.withColumn("ORDER", F.col("validation_check_failures"))
    df = update_column_values_from_map(df, "ORDER", error_priority_map, default_value=9999)
    return df.orderBy("ORDER").drop("ORDER")


def generate_comparison_tables(
    baseline_df: DataFrame, comparison_df: DataFrame, unique_id_column: str, diff_sample_size: int = 10
) -> DataFrame:
    """
    Compares two dataframes, checking each column and row for each unique id in the baseline dataframe against
    matching columns and unique ids in the comparison df.

    Returns two dataframes describing the differences found.

    The first, counts_df, contains the count of uniqueids in each column that are different between the two dataframes,
    this is presented as both 'difference_count' and 'difference_count_non_null_change' which only counts instances
    where the value for the unique id was not null in the baseline_df.

    The second, diffs_df, contains diff_sample_size unique ids for each column that is not identical in both the baseline
    and comparison dataframes.

    Parameters
    ----------
    baseline_df : DataFrame
        baseline dataframe to compare against, only unique ids and columns in this dataframe will be compared
    comparison_df : DataFrame
        comparison dataframe to compare against the baseline_df
    unique_id_column : str
        unique id column to use as a reference between both the baseline_df and comparison_df
    diff_sample_size : int, optional
        number of unique ids to return for non-identical columns in diffs_df output, by default 10

    Returns
    -------
    counts_df : DataFrame
        _description_
    diffs_df : DataFrame

    """
    window = Window.partitionBy("column_name").orderBy("column_name")
    cols_to_check = [col for col in baseline_df.columns if col in comparison_df.columns and col != unique_id_column]

    for col in cols_to_check:
        baseline_df = baseline_df.withColumnRenamed(col, f"{col}_ref")

    df = baseline_df.join(comparison_df, on=unique_id_column, how="left")

    diffs_df = df.select(
        [
            F.when(F.col(col).eqNullSafe(F.col(f"{col}_ref")), None).otherwise(F.col(unique_id_column)).alias(col)
            for col in cols_to_check
        ]
    )
    diffs_df = diffs_df.select(
        F.explode(
            F.array(
                [
                    F.struct(F.lit(col).alias("column_name"), F.col(col).alias(unique_id_column))
                    for col in diffs_df.columns
                ]
            )
        ).alias("kvs")
    )
    diffs_df = (
        diffs_df.select("kvs.column_name", f"kvs.{unique_id_column}")
        .filter(F.col(unique_id_column).isNotNull())
        .withColumn("ROW", F.row_number().over(window))
        .filter(F.col("ROW") < diff_sample_size)
    ).drop("ROW")

    counts_df = df.select(
        *[
            F.sum(F.when(F.col(c).eqNullSafe(F.col(f"{c}_ref")), 0).otherwise(1)).alias(c).cast("integer")
            for c in cols_to_check
        ],
        *[
            F.sum(F.when((~F.col(c).eqNullSafe(F.col(f"{c}_ref"))) & (F.col(f"{c}_ref").isNotNull()), 1).otherwise(0))
            .alias(f"{c}_non_null_change")
            .cast("integer")
            for c in cols_to_check
        ],
    )
    counts_df = counts_df.select(
        F.explode(
            F.array(
                [
                    F.struct(
                        F.lit(col).alias("column_name"),
                        F.col(col).alias("difference_count"),
                        F.col(f"{col}_non_null_change").alias("difference_count_non_null_change"),
                    )
                    for col in [c for c in counts_df.columns if not c.endswith("_non_null_change")]
                ]
            )
        ).alias("kvs")
    )
    counts_df = counts_df.select("kvs.column_name", "kvs.difference_count", "kvs.difference_count_non_null_change")
    return counts_df, diffs_df


class ExcelReport:
    def __init__(self, output_directory: str = None, output_file_prefix: str = "phm_report_output"):
        """"""
        self.output_directory = output_directory
        self.output_file_prefix = output_file_prefix
        self.sheets: List[Tuple[DataFrame, str]] = []
        self.output = BytesIO()

    def add_sheet(self, df: DataFrame, sheet_name: str):
        """"""
        self.sheets.append((df, sheet_name))

    def write_excel_output(self, output_directory: str = None, output_file_prefix: str = None):
        output_directory = output_directory if output_directory else self.output_directory
        output_file_prefix = output_file_prefix if output_file_prefix else self.output_file_prefix
        with pd.ExcelWriter(self.output) as writer:
            for df, sheet_name in self.sheets:
                df.toPandas().to_excel(writer, sheet_name=sheet_name, index=False)

        write_string_to_file(
            self.output.getbuffer(),
            f"{output_directory}/{output_file_prefix}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx",
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
        )
        df = df.withColumn("DIFF", F.datediff(F.col(reference_date_column), F.col(window_start_column)) + 1)

        # find the max difference
        if not window_range:
            window_range = df.select("DIFF").rdd.max()[0]

        # create completion rate columns
        df = (
            df.groupBy(window_start_column, window_end_column)
            .pivot("DIFF")
            .agg(
                F.first("daily_" + full_column_name).alias(full_column_name),
                F.first("daily_" + partial_column_name).alias(partial_column_name),
            )
            .fillna(0)
        )
        partial_columns = [c for c in df.columns if c.endswith(partial_column_name)]
        full_columns = [c for c in df.columns if c.endswith(full_column_name)]

        partial_df = df.select(window_start_column, window_end_column, *partial_columns)
        partial_df = partial_df.withColumn("total", sum([F.col(c) for c in partial_columns]))

        for col in partial_columns:
            partial_df = partial_df.withColumnRenamed(col, col.split("_")[0])

        full_df = df.select(window_start_column, window_end_column, *full_columns)
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
        sheet_name_prefix: str = "daily",
    ):
        """"""
        df = df.withColumn(reference_date_column + "_date_component", F.to_date(reference_date_column))
        reference_date_column = reference_date_column + "_date_component"
        window_a = Window.partitionBy(window_start_column, window_end_column, reference_date_column)
        window_b = Window.partitionBy(window_start_column, window_end_column)

        df = df.select(
            participant_id_column, window_start_column, window_end_column, reference_date_column, window_status_column
        ).filter(all_columns_not_null([window_status_column, window_start_column, window_end_column]))

        df = df.withColumn(
            "daily_full_completion_count",
            F.sum(F.when(F.col(window_status_column) == "Completed", 1).otherwise(0)).over(window_a),
        )
        df = df.withColumn(
            "daily_full_completion_rate",
            F.col("daily_full_completion_count") / F.count(participant_id_column).over(window_b),
        )

        df = df.withColumn(
            "daily_partial_completion_count",
            F.sum(F.when(F.col(window_status_column) == "Partially Completed", 1).otherwise(0)).over(window_a),
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

        self.add_sheet(partial_df_count, f"{sheet_name_prefix} pc counts")
        self.add_sheet(full_df_count, f"{sheet_name_prefix} fc counts")
        self.add_sheet(partial_df_rate, f"{sheet_name_prefix} pc rates")
        self.add_sheet(full_df_rate, f"{sheet_name_prefix} fc rates")

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
        sheet_name_prefix: str = "monthly",
    ):
        """"""
        df = df.filter(all_columns_not_null([window_status_column, window_start_column, window_end_column]))
        df = self.add_start_end_delta_columns(
            df=df,
            start_column_name="START",
            end_column_name="END",
            days_range=window_range,
            start_reference_date_column=window_start_column,
            reference_date_column=reference_date_column,
        )
        partial_df_rate, full_df_rate = self.create_completion_table_days(
            df=df,
            participant_id_column=participant_id_column,
            window_start_column="START",
            window_end_column="END",
            window_status_column=window_status_column,
            reference_date_column=reference_date_column,
            window_range=window_range,
            sheet_name_prefix=sheet_name_prefix,
        )
        partial_df_rate = partial_df_rate.withColumn("date_range", F.concat_ws("-", "START", "END")).drop(
            "START", "END"
        )
        full_df_rate = full_df_rate.withColumn("date_range", F.concat_ws("-", "START", "END")).drop("START", "END")

        return partial_df_rate, full_df_rate

    def create_validated_file_list(self, df: DataFrame, source_file_column: str, sheet_name_prefix: str = "validated"):
        """
        Runs the validate_processed_files on the input df and creates dfs and then sheets to add to the report object
        """
        spark_session = get_or_create_spark_session()
        processed_files, unprocessed_files, non_existent_files = validate_processed_files(df, source_file_column)
        if processed_files:
            processed_df = spark_session.createDataFrame(pd.DataFrame(processed_files, columns=["file_path"]))
            self.add_sheet(processed_df, f"{sheet_name_prefix} processed file paths")
        if unprocessed_files:
            unprocessed_df = spark_session.createDataFrame(pd.DataFrame(unprocessed_files, columns=["file_path"]))
            self.add_sheet(unprocessed_df, f"{sheet_name_prefix} unprocessed file paths")
        if non_existent_files:
            non_existent_df = spark_session.createDataFrame(pd.DataFrame(non_existent_files, columns=["file_path"]))
            self.add_sheet(non_existent_df, f"{sheet_name_prefix} nonexistent file paths")
