from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cishouseholds.compare import prepare_for_union
from cishouseholds.edit import rename_column_names
from cishouseholds.pipeline.load import update_table


def flag_identical_rows_after_first(df: DataFrame, exclusion_columns: Union[List[str], str], drop_flag_column: str):
    """
    Assign a flag for rows after first that contain duplicated data across selected columns
    Parameters
    ----------
    df
    exclusion_columns
    drop_flag_column
    """
    if type(exclusion_columns) != list:
        exclusion_columns = [exclusion_columns]  # type: ignore
    columns = df.columns
    for col in [drop_flag_column, *exclusion_columns]:
        if col in columns:
            columns.remove(col)
    window = Window.partitionBy(*columns).orderBy(exclusion_columns[0])
    df = df.withColumn("ROW_NUMBER", F.row_number().over(window))
    df = df.withColumn(drop_flag_column, F.when(F.col("ROW_NUMBER") != 1, 1).otherwise(F.col(drop_flag_column)))
    return df.drop("ROW_NUMBER")


def union_dataframes_to_hive(output_table_name: str, dataframe_list: List[DataFrame]) -> None:
    """
    Sequentially append a list of dataframes to a new HIVE table. Overwrites if table exists.
    Columns are made consistent between dataframes, filling Nulls where columns were not present on all dataframes.
    """
    dataframes = prepare_for_union(tables=dataframe_list)

    update_table(dataframes[0], output_table_name, mode_overide="overwrite")
    for df in dataframes[1:]:
        update_table(df, output_table_name, mode_overide="append")


def union_multiple_tables(tables: List[DataFrame]):
    """
    Given a list of tables combine them through a union process
    and create null columns for columns inconsistent between all tables
    Parameters
    ----------
    tables
        list of objects representing the respective input tables
    """
    dataframes = prepare_for_union(tables=tables)
    merged_df = dataframes[0]
    for dfn in dataframes[1:]:
        merged_df = merged_df.union(dfn)
    return merged_df


def join_assayed_bloods(df: DataFrame, test_target_column: str, join_on_columns: Union[str, List[str]]):
    """
    Given a dataframe containing records for both blood groups create a new dataframe with columns for
    each specific blood group seperated with the appropriate extension appended to the end of the
    column name.
    Parameters
    ----------
    df
    test_target_column
        column containing blood test target protein
    join_on_columns
        list of column names to join on, where first name is the unique ID
    """
    window = Window.partitionBy(join_on_columns)
    df = df.withColumn("sum", F.count("blood_sample_barcode").over(window))

    failed_df = df.filter(F.col("sum") > 2).drop("sum")
    df = df.filter(F.col("sum") < 3).drop("sum")

    split_dataframes = {}
    for blood_group in ["S", "N"]:
        split_df = df.filter(F.col(test_target_column) == blood_group)
        new_column_names = {
            col: col + "_" + blood_group.lower() + "_protein" if col not in join_on_columns else col
            for col in split_df.columns
        }
        split_df = rename_column_names(split_df, new_column_names)
        split_dataframes[blood_group] = split_df

    joined_df = null_safe_join(
        split_dataframes["N"],
        split_dataframes["S"],
        null_safe_on=list(join_on_columns[1:]),
        null_unsafe_on=[join_on_columns[0]],
        how="outer",
    )
    joined_df = joined_df.drop(test_target_column + "_n_protein", test_target_column + "_s_protein")
    return joined_df, failed_df


def null_safe_join(left_df: DataFrame, right_df: DataFrame, null_safe_on: list, null_unsafe_on: list, how="left"):
    """
    Performs a join on equal columns, where a subset of the join columns can be null safe.

    Parameters
    ----------
    left_df
    right_df
    null_safe_on
        columns to make a null safe equals comparison on
    null_unsafe_on
        columns to make a null unsafe equals comparison on
    how
        join type
    """
    for column in null_safe_on + null_unsafe_on:
        right_df = right_df.withColumnRenamed(column, column + "_right")

    null_unsafe_conditions = [f"({column} == {column + '_right'})" for column in null_unsafe_on]
    null_safe_conditions = [f"({column} <=> {column + '_right'})" for column in null_safe_on]

    join_condition = " AND ".join(null_unsafe_conditions + null_safe_conditions)

    joined_df = left_df.join(right_df, on=F.expr(join_condition), how=how)

    for column in null_safe_on + null_unsafe_on:
        # Recover right entries from outer/right joins
        joined_df = joined_df.withColumn(column, F.coalesce(F.col(column), F.column(column + "_right")))

    return joined_df.drop(*[column + "_right" for column in null_safe_on + null_unsafe_on])


def assign_count_of_occurrences_column(df: DataFrame, reference_column: str, column_name_to_assign: str):
    """
    Derive column to count the number of occurrences of the value in the reference_column over the entire dataframe.
    Parameters
    ----------
    df
    reference_column
        Name of column to count value occurrences
    column_name_to_assign
        Name of column to be created
    """
    window = Window.partitionBy(reference_column)

    return df.withColumn(column_name_to_assign, F.count(reference_column).over(window).cast("integer"))


def assign_absolute_offset(df: DataFrame, column_name_to_assign: str, reference_column: str, offset: float):
    """
    Assign column based on the absolute value of an offsetted number.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    reference_column
        Name of column to calculate values for new column from
    offset
        Amount to offset each reference_column value by
    Notes
    -----
    Offset will be subtracted.
    """
    return df.withColumn(column_name_to_assign, F.abs(F.col(reference_column) - offset))


def assign_group_and_row_number_columns(
    df: DataFrame, window: Window, row_num_column: str, group_column: str, group_by_column: str
):
    """
    create columns for row number and group number of rows within a window

    Parameters
    ----------
    df
    window
    group_by_column

    """
    df = df.withColumn(row_num_column, F.row_number().over(window))
    dft = df.withColumnRenamed(group_by_column, "b").groupBy("b").count()
    dft = dft.withColumn("dummy", F.lit(1).cast("integer"))
    mini_window = Window.partitionBy("dummy").orderBy("b")
    dft = dft.withColumn(group_column, F.row_number().over(mini_window))
    df = df.join(dft, dft.b == F.col(group_by_column)).drop("b", "dummy")
    return df


def flag_rows_different_to_reference_row(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    row_column: str,
    group_column: str,
    exclusion_row_value: int,
):
    """
    create flag column for rows where value differs from that of first row in group

    Parameters
    ----------
    df
    column_name_to_assign
    reference_column
    check_column
    selection_column
    exclusion_column
    exclusion_row_value

    """
    df_reference_row_and_col_only = df.filter(F.col(row_column) == exclusion_row_value).select(
        group_column, reference_column
    )
    df = df_reference_row_and_col_only.join(
        df.withColumnRenamed(reference_column, reference_column + "_reference"), on=[group_column], how="inner"
    )
    df = df.withColumn(column_name_to_assign, F.lit(None).cast("integer"))
    df = df.withColumn(
        column_name_to_assign, F.when(F.col(reference_column + "_reference") != F.col(reference_column), 1).otherwise(0)
    )
    return df.drop(reference_column + "_reference")


def check_consistency_in_retained_rows(
    df: DataFrame, check_columns: List[str], selection_column: str, group_column: str, column_name_to_assign: str
):
    """
    check consistency of multiply columns and create separate joined dataframe of chosen columns

    Parameters
    ----------
    df
    check_columns
    selection_column
    group_column
    column_name_to_assign
    """
    check_distinct = []
    for col in check_columns:
        df_grouped = (
            df.groupBy(group_column, col)
            .count()
            .withColumnRenamed("count", "num_objs")
            .groupBy(group_column)
            .count()
            .withColumnRenamed("count", "num_" + col + "_distinct")
        )
        df = df.join(df_grouped, on=[group_column], how="inner")
        check_distinct.append("num_" + col + "_distinct")
    columns = [F.col(col) for col in check_distinct]
    df = df.withColumn("array_distinct", F.array(columns)).drop(*check_distinct)
    df = df.withColumn(
        column_name_to_assign, F.when(F.array_contains("array_distinct", 2), 1).otherwise(0).cast("integer")
    )
    return df.drop("num_objs", "array_distinct")


def assign_time_difference_and_flag_if_outside_interval(
    df: DataFrame,
    column_name_outside_interval_flag: str,
    column_name_time_difference: str,
    start_datetime_reference_column: str,
    end_datetime_reference_column: str,
    interval_lower_bound: Union[int, float],
    interval_upper_bound: Union[int, float],
    interval_bound_format: str = "hours",
) -> DataFrame:
    """
    Creates column to give the time difference in either hours (by default) or days
    between two columns, and creates associated column to flag whether the difference is
    between specified lower and upper bounds (inclusive). If the difference is outside
    of these bounds, return 1, otherwise None.
    Parameters
    ----------
    df
    column_name_outside_interval_flag
        Name of the column that returns whether the difference in datetimes is
        within the upper/lower bounds. If within, return None, otherwise
        an integer 1.
    column_name_time_difference
        Name of the column that returns the difference between start and end
        datetimes
    start_datetime_reference_column
        Reference column with datetime in string format yyyy-mm-dd hh:mm:ss.
    end_datetime_reference_column
        Reference column with datetime in string format yyyy-mm-dd hh:mm:ss.
    interval_lower_bound
        The minimum accepted time difference interval between
        end_datetime_reference_column and start_datetime_reference_column.
    interval_upper_bound
        The maximum accepted time difference interval between
        end_datetime_reference_column and start_datetime_reference_column
    interval_bound_format
        By default will be a string called 'hours'. If upper and lower interval
        bounds are input as days, define interval_format to 'days'.
        These are the only two possible formats.
    Notes
    -----
    Lower_interval should be a negative value if start_datetime_reference_column
    is after end_datetime_reference_column.
    """

    if interval_bound_format == "hours":
        conversion_factor = 3600  # 1h has 60s*60min seconds = 3600 seconds
    elif interval_bound_format == "days":
        conversion_factor = 86400  # 1 day has 60s*60min*24h seconds = 86400 seconds

    # FORMULA: (end_datetime_reference_column - start_datetime_reference_column) in
    # seconds/conversion_factor in seconds
    df = df.withColumn(
        column_name_time_difference,
        (
            F.to_timestamp(F.col(end_datetime_reference_column)).cast("long")
            - F.to_timestamp(F.col(start_datetime_reference_column)).cast("long")
        )
        / conversion_factor,
    )

    return df.withColumn(
        column_name_outside_interval_flag,
        F.when(~F.col(column_name_time_difference).between(interval_lower_bound, interval_upper_bound), 1)
        .otherwise(None)
        .cast("integer"),
    )


def assign_merge_process_group_flag(
    df: DataFrame,
    column_name_to_assign: str,
    out_of_date_range_flag: str,
    count_barcode_labs_column_name: str,
    count_barcode_labs_condition: str,
    count_barcode_voyager_column_name: str,
    count_barcode_voyager_condition: str,
):
    """
    Combine three conditions to create flag indicating record to be processed in forthcoming matching process.
    This is run for each of 1:1, 1:many, many:1 and many:many to identify the relevant processing group.
    Parameters
    ----------
    df
    column_name_to_assign
    out_of_date_range_flag
    count_barcode_labs_column_name
    count_barcode_labs_condition
    count_barcode_voyager_column_name
    count_barcode_voyager_condition
    """

    count_barcode_labs_condition = F.expr(f"{count_barcode_labs_column_name} {count_barcode_labs_condition}")
    count_barcode_voyager_condition = F.expr(f"{count_barcode_voyager_column_name} {count_barcode_voyager_condition}")

    df = df.withColumn("count_barcode_labs_flag", F.when(count_barcode_labs_condition, 1))
    df = df.withColumn("count_barcode_voyager_flag", F.when(count_barcode_voyager_condition, 1))

    return df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("count_barcode_labs_flag") + F.col("count_barcode_voyager_flag") == 2),
            1,
        )
        .otherwise(None)
        .cast("integer"),
    ).drop("count_barcode_labs_flag", "count_barcode_voyager_flag")


def many_to_one_swab_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str, ordering_columns: list):
    """
    Many (Voyager) to one (swab) matching process.
    Creates flag for records to be dropped as non-optimal matches.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to flag records to be 'dropped'
    group_by_column
        Name of columns to group dataframe
    ordering_columns
        Names of columns to order each group
    """

    # Row number won't apply with frame set to unbounded (rowsBetween)
    bounded_window = Window.partitionBy(group_by_column).orderBy(*ordering_columns)
    df = df.withColumn("row_number", F.row_number().over(bounded_window))
    unbounded_window = (
        Window.partitionBy(group_by_column)
        .orderBy(*ordering_columns)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = df.withColumn(
        "count_diff_same_as_first",
        F.sum(
            F.when(
                F.col("diff_vs_visit_hr_swab") == F.first("diff_vs_visit_hr_swab").over(unbounded_window), 1
            ).otherwise(None)
        )
        .over(unbounded_window)
        .cast("integer"),
    )
    df = df.withColumn(
        "diff_between_first_and_second_records",
        F.sum(
            F.when(
                F.col("row_number") == 2,
                F.col("diff_vs_visit_hr_swab") - F.first("diff_vs_visit_hr_swab").over(unbounded_window),
            )
            .otherwise(None)
            .cast("integer")
        ).over(unbounded_window),
    )
    df = df.withColumn(
        "abs_offset_diff_between_first_and_second_records",
        F.sum(
            F.when(
                F.col("row_number") == 2,
                F.col("abs_offset_diff_vs_visit_hr_swab")
                - F.first("abs_offset_diff_vs_visit_hr_swab").over(unbounded_window),
            )
            .otherwise(None)
            .cast("integer")
        ).over(unbounded_window),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.count(group_by_column).over(unbounded_window) == F.col("count_diff_same_as_first")),
            1,
        )
        .otherwise(None)
        .cast("integer"),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.abs(F.col("diff_between_first_and_second_records")) < 8),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("abs_offset_diff_between_first_and_second_records") >= 8)
            & (F.first("diff_vs_visit_hr_swab").over(unbounded_window) >= 0)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("abs_offset_diff_between_first_and_second_records") >= 8)
            & (F.first("diff_vs_visit_hr_swab").over(unbounded_window) < 0)
            & (F.col("diff_between_first_and_second_records") > 48)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.first("diff_vs_visit_hr_swab").over(unbounded_window).between(7, 20))
            & (F.col("diff_between_first_and_second_records") > 16)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("abs_offset_diff_between_first_and_second_records") <= 8) & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    return df.drop(
        "row_number",
        "count_occurrences",
        "count_diff_same_as_first",
        "diff_between_first_and_second_records",
        "abs_offset_diff_between_first_and_second_records",
    )


def many_to_one_antibody_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str):
    """
    Many (Voyager) to one (antibody) matching process. Creates a flag to identify rows which doesn't match
    required criteria (to be filtered later)
    Parameters
    ----------
    df
    column_name_to_assign
    """

    window = Window.partitionBy(group_by_column, "out_of_date_range_antibody")

    df = df.withColumn(
        "antibody_barcode_cleaned_count",
        F.count(F.col(group_by_column)).over(window),
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col("antibody_barcode_cleaned_count") > 1) | (F.col("out_of_date_range_antibody") == 1), 1)
        .otherwise(None)
        .cast("integer"),
    )
    return df.drop("antibody_barcode_cleaned_count")


def many_to_many_flag(
    df: DataFrame,
    drop_flag_column_name_to_assign: str,
    out_of_date_range_column: str,
    group_by_column: str,
    ordering_columns: list,
    process_type: str,
    failure_column_name: str,
):
    """
    Many (Voyager) to Many (antibody) matching process.
    Creates flag for records to be dropped as non-optimal matches, and separate flag for records where process fails.
    Parameters
    ----------
    df
    drop_flag_column_name_to_assign
        Name of column to indicate record is to be dropped
    group_by_column
        Names of columns to group dataframe
    ordering_columns
        Names of columns to order each group
    process_type
        Defines which many-to-many matching process is being carried out.
        Must be 'antibody' or 'swab'
    failure_column_name
        Name of column to indicate record has failed validation logic
    """

    window = Window.partitionBy(group_by_column)
    process_type_id_map = {"antibody": "unique_antibody_test_id", "swab": "unique_pcr_test_id"}
    unique_id_lab_str = process_type_id_map[process_type]

    if process_type == "antibody":
        column_to_validate = "antibody_test_result_classification_s_protein"
    elif process_type == "swab":
        column_to_validate = "pcr_result_classification"

    df = df.withColumn(
        "classification_different_to_first",
        F.sum(
            F.when(F.col(column_to_validate) == F.first(column_to_validate).over(window), None)
            .otherwise(1)
            .cast("integer")
        ).over(window),
    )

    df = df.withColumn(
        failure_column_name,
        F.when(
            F.last("classification_different_to_first").over(window).isNotNull(),
            1,
        )
        .otherwise(None)
        .cast("integer"),
    )

    df = df.withColumn("record_processed", F.lit(None).cast("integer"))

    df = df.withColumn(drop_flag_column_name_to_assign, F.lit(None).cast("integer"))

    for _ in range(2):
        window = Window.partitionBy(group_by_column, "record_processed").orderBy(*ordering_columns)
        df = df.withColumn("row_number", F.row_number().over(window))
        df = df.withColumn(
            "record_processed",
            F.when(
                (
                    (F.col(unique_id_lab_str) == (F.first(unique_id_lab_str).over(window)))
                    | (
                        F.col("unique_participant_response_id")
                        == (F.first("unique_participant_response_id").over(window))
                    )
                )
                & (F.col("row_number") != 1),
                1,
            ).otherwise(F.col("record_processed")),
        )

        df = df.withColumn(
            drop_flag_column_name_to_assign,
            F.when((F.col("record_processed") == 1) | (F.col(out_of_date_range_column) == 1), 1).otherwise(None),
        )

        df = df.withColumn(
            "record_processed",
            F.when((F.col("row_number") == 1) & (F.col(drop_flag_column_name_to_assign).isNull()), 0).otherwise(
                F.col("record_processed")
            ),
        )
    return df.drop("classification_different_to_first", "record_processed", "row_number")


def one_to_many_antibody_flag(
    df: DataFrame,
    column_name_to_assign: str,
    group_by_column: str,
    diff_interval_hours: str,
    siemens_column: str,
    tdi_column: str,
    visit_date: str,
) -> DataFrame:
    """
    steps to complete:
    Step 1: create columns for row number and group number in new grouped df
    Step 2: add first diff interval ref and flag records with different diff interval to first
    Step 3: check for consistent siemens, tdi data within groups of matched barcodes
    Step 4: add drop flag 2 column to indicate inconsistent siemens or tdi data within group
    Step 5: create overall drop flag to drop any records which have inconsistent data or
    interval different to that of first row reference
    Parameters
    ----------
    df
    column_name_to_assign
    group_by_column
    diff_interval_hours
    siemens_column
    tdi_column
    visit_date
    """
    df = df.withColumn("abs_diff_interval", F.abs(F.col(diff_interval_hours)))
    row_num_column = "row_num"
    group_num_column = "group_num"
    unique_antibody_id_column = "unique_antibody_test_id"
    diff_interval_hours = "abs_diff_interval"
    rows_diff_to_ref = "rows_diff_to_ref_flag"
    inconsistent_rows = "failed_flag_1tom_antibody"  # column generated that isnt included in the TEST data.

    window = Window.partitionBy(group_by_column).orderBy(diff_interval_hours, visit_date)
    df = assign_group_and_row_number_columns(df, window, row_num_column, group_num_column, group_by_column)
    df = flag_rows_different_to_reference_row(
        df, rows_diff_to_ref, diff_interval_hours, row_num_column, group_num_column, 1
    )
    df = check_consistency_in_retained_rows(
        df, [siemens_column, tdi_column], rows_diff_to_ref, group_num_column, inconsistent_rows
    )
    df = df.withColumn(
        column_name_to_assign,
        F.when((F.col(rows_diff_to_ref) == 1), 1).otherwise(None),
    )
    df = flag_identical_rows_after_first(
        df,
        exclusion_columns=[unique_antibody_id_column, group_num_column, row_num_column],
        drop_flag_column=column_name_to_assign,
    )
    return df.drop(row_num_column, group_num_column, diff_interval_hours, rows_diff_to_ref, "count")


def one_to_many_swabs(
    df: DataFrame,
    group_by_column: str,
    ordering_columns: List[str],
    pcr_result_column_name: str,
    void_value: Union[str, int],
    flag_column_name: str,
) -> DataFrame:
    """
    One (Voyager) to Many (Antibody) matching process. Creates a flag to identify rows which do not match
    required criteria to be filtered out. The required criteria is a combination of 4 steps
    or subfunctions that creates one column for each step. And a combination of these 4 steps/columns
    are used to create the final one to many swabs column.
    Step 1: uses assign_merge_process_group_flag()
    Step 2: uses merge_one_to_many_swab_ordering_logic()
    Step 3: uses merge_one_to_many_swab_result_pcr_logic()
    Step 4: uses merge_one_to_many_swab_time_difference_logic()
    Parameters
    ----------
    df
    out_of_date_range_flag
        Column to work out whether the date difference is within an upper/lower range. This column can be
        created by assign_time_difference_and_flag_if_outside_interval() by inputing two date columns.
    count_barcode_labs_column_name
        the Many column from One-to-Many
    count_barcode_voyager_column_name
        the One column from One-to-Many
    group_by_column
        the barcode, user_id used for grouping for the steps 2, 3, 4 where window function is used
    ordering_columns
        a list of strings with the column name for ordering used in steps 2 and 4.
    pcr_result_column_name
    void_value
        value given the pcr_result_name for void that could be a string 'void' or a number.
    flag_column_name
        Combination of steps 1, 2, 3, 4 using a OR boolean operation to apply the whole One-to-Many swabs
    Notes
    -----
    The Specific order for ordering_columns used was abs(date_diff - 24), date_difference, date.
    """
    df = df.withColumn(flag_column_name, F.lit(None).cast("string"))
    df = merge_one_to_many_swab_result_pcr_logic(
        df=df,
        void_value=void_value,
        group_by_column=group_by_column,
        pcr_result_column_name=pcr_result_column_name,
        result_pcr_logic_flag_column_name="pcr_flag",
    )
    df = merge_one_to_many_swab_ordering_logic(
        df=df,
        group_by_column=group_by_column,
        ordering_columns=ordering_columns,
        time_order_logic_flag_column_name="time_order_flag",
    )

    df = merge_one_to_many_swab_time_difference_logic(
        df=df,
        group_by_column=group_by_column,
        ordering_columns=ordering_columns,
        time_difference_logic_flag_column_name="time_difference_flag",
    )

    df = df.withColumn(
        "time_order_flag", F.when(F.col("pcr_flag") == 1, F.lit(None)).otherwise(F.col("time_order_flag"))
    )

    df = df.withColumn(
        "time_difference_flag", F.when(F.col("pcr_flag") == 1, F.lit(None)).otherwise(F.col("time_difference_flag"))
    )

    df = df.withColumn(
        flag_column_name, F.when(F.col("pcr_flag") == 1, 1).otherwise(F.col(flag_column_name)).cast("integer")
    )

    # column: time_difference_flag
    common_condition = F.col("time_difference_flag").isNull()

    w_rank = Window.partitionBy(F.col(group_by_column), F.when(common_condition, 1).otherwise(0)).orderBy(
        F.col("time_difference_flag")
    )

    df = df.withColumn(
        "time_difference_flag", F.when(common_condition, F.lit(None)).otherwise(F.rank().over(w_rank))
    ).withColumn("time_difference_flag", F.when(F.col("time_difference_flag") == 1, None).otherwise(1))

    # column: time_order_flag
    common_condition = F.col("time_order_flag").isNull()

    w_rank = Window.partitionBy(F.col(group_by_column), F.when(common_condition, 1).otherwise(0)).orderBy(
        F.col("time_order_flag")
    )

    df = df.withColumn(
        "time_order_flag", F.when(common_condition, F.lit(None)).otherwise(F.rank().over(w_rank))
    ).withColumn("time_order_flag", F.when(F.col("time_order_flag") == 1, None).otherwise(1))

    df = df.withColumn(
        flag_column_name,
        F.when((F.col("time_order_flag") == 1) & (F.col("time_difference_flag") == 1), 1)
        .when((F.col("time_order_flag") == 1) & (F.col("time_difference_flag").isNull()), None)
        .when((F.col("time_order_flag").isNull()) & (F.col("time_difference_flag") == 1), 1)
        .otherwise(F.col(flag_column_name)),
    )
    # Solve case for both time diff negative and positive within barcode
    df = df.withColumn(
        "aux_neg", F.count(F.when(F.col("diff_vs_visit_hr_swab") < 0, 1)).over(Window.partitionBy(group_by_column))
    )
    df = df.withColumn(
        "aux_pos", F.count(F.when(F.col("diff_vs_visit_hr_swab") >= 0, 1)).over(Window.partitionBy(group_by_column))
    )

    df = df.withColumn(
        flag_column_name,
        F.when((F.col("aux_neg") > 0) & (F.col("aux_pos") > 0), F.lit(None)).otherwise(F.col(flag_column_name)),
    ).drop("aux_neg", "aux_pos")

    return df


def merge_one_to_many_swab_ordering_logic(
    df: DataFrame, group_by_column: str, ordering_columns: List[str], time_order_logic_flag_column_name: str
) -> DataFrame:
    """
    Step 2: applies an ordering function with the parameter ordering_columns
    (a list of strings with the table column names) to organise ascending each record
    within a window and ranks the order from earliest being 1, to latest being a
    higher number.
    Parameters
    ----------
    df
    group_by_column
        the barcode, user_id used for grouping for the steps 2, 3, 4 where window function is used
    ordering_columns
        a list of strings with the column name for ordering used in steps 2 and 4.
    time_order_logic_flag_column_name
        Output column in Step 2
    """
    window = Window.partitionBy(group_by_column).orderBy(*ordering_columns)
    return df.withColumn(time_order_logic_flag_column_name, F.rank().over(window).cast("integer"))


def merge_one_to_many_swab_result_pcr_logic(
    df: DataFrame,
    void_value: Union[str, int],
    group_by_column: str,
    pcr_result_column_name: str,
    result_pcr_logic_flag_column_name: str,
) -> DataFrame:
    """
    Step 3: drops result pcr if there are void values having at least a positive/negative within the
    same barcode.
    ----------
    df
    void_value
        the way void is represented in column pcr_result
    group_by_column
        the barcode, user_id used for grouping for the steps 2, 3, 4 where window function is used
    pcr_result_column_name
    result_pcr_logic_flag_column_name
        Output column in Step 3
    """
    df_output = df

    df = df.withColumn(
        "other_than", F.when(~(F.col(pcr_result_column_name) == void_value), 1).cast("integer")
    ).withColumn(void_value, F.when(F.col(pcr_result_column_name) == void_value, 1))

    df = (
        df.dropDuplicates([group_by_column, "other_than", void_value])
        .drop(pcr_result_column_name)
        .groupBy(group_by_column)
        .agg(F.sum("other_than").alias("other_than"), F.sum(void_value).alias(void_value))
        .withColumn(
            result_pcr_logic_flag_column_name,
            F.when((F.col("other_than").isNotNull()) & (F.col(void_value).isNotNull()), 1).cast("integer"),
        )
        .select(group_by_column, result_pcr_logic_flag_column_name)
    )

    return df_output.join(df, [group_by_column], "inner").withColumn(
        result_pcr_logic_flag_column_name,
        F.when((F.col(pcr_result_column_name) == void_value) & (F.col(result_pcr_logic_flag_column_name) == 1), 1)
        .otherwise(None)
        .cast("integer"),
    )


def merge_one_to_many_swab_time_difference_logic(
    df: DataFrame, group_by_column: str, ordering_columns: List[str], time_difference_logic_flag_column_name: str
) -> DataFrame:
    """
    Step 4: After having ordered in Step 2 by the ordering_columns, if the first record has a positive date
    difference, flag all the other records. And if the first record has a negative time difference,
    ensuring that the following records have also a negative time difference, flag the one after the first
    to be dropped.
    Parameters
    ----------
    df
    ordering_columns
        a list of strings with the column name for ordering used in steps 2 and 4.
    time_difference_logic_flag_column_name
        Output column in Step 4
    """
    window = Window.partitionBy(group_by_column).orderBy(*ordering_columns)

    return df.withColumn(time_difference_logic_flag_column_name, F.rank().over(window))
