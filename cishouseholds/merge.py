from typing import List
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def merge_assayed_bloods(df: DataFrame, blood_group_column: str):
    """
    Given a dataframe containing records for both blood groups create a new dataframe with columns for
    each specific blood group seperated with the appriopiate extension appended to the end of the
    column name
    Parameters
    ----------
    df
    blood_group_column
    """
    join_on_colums = ["blood_sample_barcode", "antibody_test_plate_number", "antibody_test_well_id"]
    split_dataframes = []
    for blood_group in ["S", "N"]:
        split_df = df.filter(F.col(blood_group_column) == blood_group)
        for col in split_df.columns:
            if col not in join_on_colums:
                split_df = split_df.withColumnRenamed(col, col + "_" + blood_group.lower())
        split_dataframes.append(split_df)
    joined_df = join_dataframes(df1=split_dataframes[0], df2=split_dataframes[1], on=join_on_colums)
    return joined_df


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


def assign_group_and_row_number_columns(df: DataFrame, window: Window, group_by_column: str):
    """
    create columns for row number and group number of rows within a window

    Parameters
    ----------
    df
    window
    group_by_column

    """
    df = df.withColumn("row_num", F.row_number().over(window))
    dft = df.groupBy(group_by_column).count().withColumnRenamed(group_by_column, "b")
    dft = dft.withColumn("dummy", F.lit(1))
    mini_window = Window.partitionBy("dummy").orderBy("b")
    dft = dft.withColumn("group", F.row_number().over(mini_window))
    df = df.join(dft, dft.b == F.col(group_by_column)).drop("b", "dummy")
    return df, dft


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


def flag_columns_different_to_ref(
    df: DataFrame,
    column_name_to_assign: str,
    reference_column: str,
    check_column: str,
    selection_column: str,
    exclusion_column: str,
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
    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (
                (F.col(selection_column) == 1)
                & (F.col(reference_column) != F.col(check_column))
                & (F.col(exclusion_column) != exclusion_row_value)
            ),
            1,
        ).otherwise(
            None
        ),  # drop rows after first where diff vs visit is
    )
    return df


def create_count_group(df: DataFrame, group_column: str, reference_column: str, rename_group: bool):
    """
    create seperate grouped dataframe to hold number of occurances of reference column value within group

    Parameters
    ----------
    df
    group_column
    reference_column
    rename_group

    """
    dft = (
        df.where(df.identify_one_to_many_bloods_flag == 1)
        .groupBy(group_column, reference_column)
        .count()
        .withColumnRenamed(reference_column, reference_column[0])
        .withColumnRenamed("count", "c{}".format(reference_column[0]))
    )
    if rename_group:
        dft = dft.withColumnRenamed("group", "g")
    return dft


def check_consistent_data(df: DataFrame, check_column1: str, check_column2: str, group_by_column: str):
    """
    check consistency of multipl columns and create seperate joined dataframe of chosen columns

    Parameters
    ----------
    df
    check_column1
    check_column2
    group_by_column

    """
    dft1 = create_count_group(df, group_by_column, check_column1, True)
    dft2 = create_count_group(df, group_by_column, check_column2, False)
    dfj = dft1.join(dft2, (dft1.g == dft2.group)).drop("group")
    return dfj


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
        F.when(~F.col(column_name_time_difference).between(interval_lower_bound, interval_upper_bound), 1).otherwise(
            None
        ),
    )


def assign_unique_identifier_column(df: DataFrame, column_name_to_assign: str, ordering_columns: list):
    """
    Derive column with unique identifier for each record.
    Parameters
    ----------
    df
    column_name_to_assign
        Name of column to be created
    ordering_columns
        Columns to define order of records to assign an integer value from 1 onwards
        This order is mostly for comparison/proving purposes with stata output
    """

    window = Window.orderBy(*ordering_columns)
    return df.withColumn(column_name_to_assign, F.row_number().over(window))


def join_dataframes(df1: DataFrame, df2: DataFrame, on: Union[str, List[str]], join_type: str = "outer"):
    """
    Join two datasets.
    Parameters
    ----------
    df1
    df2
    reference_column
        Column for join to occur on
    join_type
        Specify join type to apply to .join() method
    """
    return df1.join(df2, on=on, how=join_type)


def assign_first_row_value_ref(df: DataFrame, reference_column: str, group_column: str, row_column, row_num: int):
    """
    create reference column for the first row of each partion in a window

    Parameters
    ----------
    df
    reference_column
    group_column
    row_column
    row_num

    """
    dft = (
        df.select(F.col(group_column), F.col(row_column), F.col(reference_column))
        .filter(F.col(row_column) == row_num)
        .withColumnRenamed(group_column, "g")
        .withColumnRenamed(reference_column, "{}{}_ref".format(reference_column[0], row_num))
        .drop(row_column)
    )
    df = df.join(dft, dft.g == F.col(group_column)).drop("g")
    return df, "{}{}_ref".format(reference_column[0], row_num)


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
            (
                F.col(out_of_date_range_flag).isNull()
                & (F.col("count_barcode_labs_flag") + F.col("count_barcode_voyager_flag") == 2)
            ),
            1,
        ).otherwise(None),
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

    df = assign_merge_process_group_flag(
        df,
        "identify_many_to_one_swab_flag",
        "out_of_date_range_swab",
        "count_barcode_swab",
        "==1",
        "count_barcode_voyager",
        ">1",
    )

    # Row number won't apply with frame set to unbounded (rowsBetween)
    bounded_window = Window.partitionBy(group_by_column, "identify_many_to_one_swab_flag").orderBy(*ordering_columns)
    df = df.withColumn("row_number", F.row_number().over(bounded_window))
    unbounded_window = (
        Window.partitionBy(group_by_column, "identify_many_to_one_swab_flag")
        .orderBy(*ordering_columns)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = df.withColumn(
        "count_diff_same_as_first",
        F.sum(
            F.when(F.col("diff_vs_visit_hr") == F.first("diff_vs_visit_hr").over(unbounded_window), 1).otherwise(None)
        ).over(unbounded_window),
    )
    df = df.withColumn(
        "diff_between_first_and_second_records",
        F.sum(
            F.when(
                F.col("row_number") == 2, F.col("diff_vs_visit_hr") - F.first("diff_vs_visit_hr").over(unbounded_window)
            ).otherwise(None)
        ).over(unbounded_window),
    )
    df = df.withColumn(
        "abs_offset_diff_between_first_and_second_records",
        F.sum(
            F.when(
                F.col("row_number") == 2,
                F.col("abs_offset_diff_vs_visit_hr") - F.first("abs_offset_diff_vs_visit_hr").over(unbounded_window),
            ).otherwise(None)
        ).over(unbounded_window),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.count(group_by_column).over(unbounded_window) == F.col("count_diff_same_as_first"))
            & (F.col("identify_many_to_one_swab_flag") == 1),
            1,
        ).otherwise(None),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.abs(F.col("diff_between_first_and_second_records")) < 8)
            & (F.col("identify_many_to_one_swab_flag") == 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("abs_offset_diff_between_first_and_second_records") >= 8)
            & (F.first("diff_vs_visit_hr").over(unbounded_window) >= 0)
            & (F.col("identify_many_to_one_swab_flag") == 1)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.col("abs_offset_diff_between_first_and_second_records") >= 8)
            & (F.first("diff_vs_visit_hr").over(unbounded_window) < 0)
            & (F.col("diff_between_first_and_second_records") > 48)
            & (F.col("identify_many_to_one_swab_flag") == 1)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (F.first("diff_vs_visit_hr").over(unbounded_window).between(7, 20))
            & (F.col("diff_between_first_and_second_records") > 16)
            & (F.col("identify_many_to_one_swab_flag") == 1)
            & (F.col("row_number") > 1),
            1,
        ).otherwise(F.col(column_name_to_assign)),
    )

    return df.drop(
        "row_number",
        "count_occurrences",
        "count_diff_same_as_first",
        "diff_between_first_and_second_records",
        "abs_offset_diff_between_first_and_second_records",
        "identify_many_to_one_swab_flag",
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
    df = assign_merge_process_group_flag(
        df,
        "identify_many_to_one_antibody_flag",
        "out_of_date_range_antibody",
        "count_barcode_antibody",
        "==1",
        "count_barcode_voyager",
        ">1",
    )

    window = Window.partitionBy(group_by_column)

    df = df.withColumn(
        "antibody_barcode_cleaned_count",
        F.sum(F.when(F.col("identify_many_to_one_antibody_flag") == 1, 1).otherwise(None)).over(window),
    )

    df = df.withColumn(column_name_to_assign, F.when(F.col("antibody_barcode_cleaned_count") > 1, 1).otherwise(None))

    return df.drop("antibody_barcode_cleaned_count", "identify_many_to_one_antibody_flag")


def many_to_many_flag(
    df: DataFrame,
    drop_flag_column_name_to_assign: str,
    group_by_column: str,
    ordering_columns: list,
    process_type: str,
    failed_flag_column_name_to_assign: str,
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
    failed_flag_column_name_to_assign
        Name of column to indicate record has failed validation logic
    """

    df = assign_merge_process_group_flag(
        df,
        "identify_many_to_many_flag",
        "out_of_date_range_" + process_type,
        "count_barcode_" + process_type,
        ">1",
        "count_barcode_voyager",
        ">1",
    )

    window = Window.partitionBy(group_by_column, "identify_many_to_many_flag")

    if process_type == "antibody":
        column_to_validate = "antibody_test_result_classification"
    elif process_type == "swab":
        column_to_validate = "pcr_result_classification"

    df = df.withColumn(
        "classification_different_to_first",
        F.sum(F.when(F.col(column_to_validate) == F.first(column_to_validate).over(window), None).otherwise(1)).over(
            window
        ),
    )

    df = df.withColumn(
        failed_flag_column_name_to_assign,
        F.when(
            F.last("classification_different_to_first").over(window).isNotNull()
            & (F.col("identify_many_to_many_flag") == 1),
            1,
        ).otherwise(None),
    )

    # record_processed set to 1 if evaluated and drop flag to be set, 0 if evaluated and drop flag to be None,
    # otherwise None
    df = df.withColumn("record_processed", F.when(F.col("identify_many_to_many_flag").isNull(), 0).otherwise(None))
    unique_id_lab_str = "unique_id_" + process_type

    while df.filter(df.record_processed.isNull()).count() > 0:
        window = Window.partitionBy(group_by_column, "identify_many_to_many_flag", "record_processed").orderBy(
            *ordering_columns
        )
        df = df.withColumn("row_number", F.row_number().over(window))
        df = df.withColumn(
            "record_processed",
            F.when(
                (
                    (F.col(unique_id_lab_str) == (F.first(unique_id_lab_str).over(window)))
                    | (F.col("unique_id_voyager") == (F.first("unique_id_voyager").over(window)))
                )
                & (F.col("row_number") != 1),
                1,
            ).otherwise(F.col("record_processed")),
        )

        df = df.withColumn(drop_flag_column_name_to_assign, F.when(F.col("record_processed") == 1, 1).otherwise(None))

        df = df.withColumn(
            "record_processed",
            F.when((F.col("row_number") == 1) & (F.col(drop_flag_column_name_to_assign).isNull()), 0).otherwise(
                F.col("record_processed")
            ),
        )

    return df.drop("identify_many_to_many_flag", "classification_different_to_first", "record_processed", "row_number")


def create_inconsistent_data_drop_flag(
    df: DataFrame, selection_column: str, item1_count_column: str, item2_count_column: str
):
    """
    create flag column for groups where data of chosen columns is inconsistent

    Parameters
    ----------
    df
    item1_count_column
    item2_count_column

    """
    df = df.withColumn(
        "dr2",
        F.when(
            (F.col(selection_column) == 1)
            & ((F.col(item1_count_column) != F.col("count")) | (F.col(item2_count_column) != F.col("count"))),
            1,
        ).otherwise(None),
    ).drop(item1_count_column, item2_count_column)
    return df


def one_to_many_bloods_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str):
    """
    steps to complete:
    create columns for row number and group number in new grouped df
    adding first diff interval ref and flagging records with different diff interval to first
    check for consistent siemens, tdi data
    adding drop flag 2 column to indicate inconsitent siemens or tdi data within group

    """
    df = assign_merge_process_group_flag(
        df,
        "identify_one_to_many_bloods_flag",
        "out_of_date_range_blood",
        "count_barcode_voyager",
        "==1",
        "count_barcode_blood",
        ">1",
    )
    window = Window.partitionBy(group_by_column).orderBy(
        "identify_one_to_many_bloods_flag", "diff_interval_hours", "visit_date"
    )
    df, dft = assign_group_and_row_number_columns(df, window, group_by_column)

    df, reference_col_name = assign_first_row_value_ref(
        df=df, reference_column="diff_interval_hours", group_column="group", row_column="row_num", row_num=1
    )

    df = flag_columns_different_to_ref(
        df=df,
        exclusion_column="row_num",
        exclusion_row_value=1,
        selection_column="identify_one_to_many_bloods_flag",
        reference_column=reference_col_name,
        check_column="diff_interval_hours",
        column_name_to_assign="dr1",
    )

    dfj = check_consistent_data(df=df, check_column1="siemens", check_column2="tdi", group_by_column="group")

    dfn = dft.join(dfj, (dfj.g == dft.group)).drop("dummy", "group", "count")

    df = df.join(
        dfn,
        (dfn.b == df.barcode_iq) & (dfn.g == df.group) & (dfn.s.eqNullSafe(df.siemens)) & (dfn.t.eqNullSafe(df.tdi)),
    ).orderBy("group", "row_num")

    df = create_inconsistent_data_drop_flag(
        df=df, selection_column="identify_one_to_many_bloods_flag", item1_count_column="cs", item2_count_column="ct"
    ).drop("s", "t", "b", "g")

    df = df.withColumn(
        column_name_to_assign,
        F.when(
            (
                (F.col("identify_one_to_many_bloods_flag") == 1)
                & (((F.col("dr2") == 1) & (F.col("row_num") != 1)) | (F.col("dr1") == 1))
            ),
            1,
        ).otherwise(None),
    )
    df = df.withColumn(
        "failed",
        F.when(
            (
                (F.col("identify_one_to_many_bloods_flag") == 1)
                & ((F.col(column_name_to_assign).isNull()) & (F.col("row_num") != 1) & (F.col("count") != 1))
            ),
            1,
        ).otherwise(None),
    )

    return df.drop(
        "out_of_date_range_blood",
        "identify_one_to_many_bloods_flag",
        "group",
        "d1_ref",
        "count",
        "row_num",
        "d1_ref",
        "dr1",
        "dr2",
    )


def one_to_many_swabs(
    df: DataFrame,
    out_of_date_range_flag: str,
    count_barcode_labs_column_name: str,
    count_barcode_voyager_column_name: str,
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
    df = assign_merge_process_group_flag(
        df,
        column_name_to_assign="merge_flag",
        out_of_date_range_flag=out_of_date_range_flag,
        count_barcode_labs_column_name=count_barcode_labs_column_name,
        count_barcode_labs_condition=">1",
        count_barcode_voyager_column_name=count_barcode_voyager_column_name,
        count_barcode_voyager_condition="==1",
    )
    df = df.withColumn(
        "merge_flag",
        F.when(F.col("merge_flag") == 1, None).otherwise(1),
    )
    df = merge_one_to_many_swab_ordering_logic(df, group_by_column, ordering_columns, "time_order_flag")
    df = merge_one_to_many_swab_result_pcr_logic(
        df=df,
        void_value=void_value,
        group_by_column=group_by_column,
        pcr_result_column_name=pcr_result_column_name,
        result_pcr_logic_flag_column_name="pcr_flag",
    )
    df = merge_one_to_many_swab_time_difference_logic(
        df=df,
        group_by_column=group_by_column,
        ordering_columns=ordering_columns,
        time_difference_logic_flag_column_name="time_difference_flag",
    )
    return df.withColumn(
        flag_column_name,
        F.when(
            (F.col("merge_flag") == 1)
            | (F.col("time_order_flag") == 1)
            | (F.col("pcr_flag") == 1)
            | (F.col("time_difference_flag") == 1),
            1,
        ),
    )


def merge_one_to_many_swab_ordering_logic(
    df: DataFrame, group_by_column: str, ordering_columns: List[str], time_order_logic_flag_column_name: str
) -> DataFrame:
    """
    Step 2: applies an ordering function with the parameter ordering_columns
    (a list of strings with the table column names) to organise ascending each record
    within a window and flag out the rows that come after the first record.
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
    return df.withColumn(time_order_logic_flag_column_name, F.rank().over(window)).withColumn(
        time_order_logic_flag_column_name,
        F.when(F.col(time_order_logic_flag_column_name) == 1, None).otherwise(1),
    )


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

    df = df.withColumn("other_than", F.when(~(F.col(pcr_result_column_name) == void_value), 1)).withColumn(
        "void", F.when(F.col(pcr_result_column_name) == void_value, 1)
    )

    df = (
        df.dropDuplicates([group_by_column, "other_than", "void"])
        .drop(pcr_result_column_name)
        .groupBy(group_by_column)
        .agg(F.sum("other_than").alias("other_than"), F.sum("void").alias("void"))
        .withColumn(
            result_pcr_logic_flag_column_name,
            F.when((F.col("other_than").isNotNull()) & (F.col("void").isNotNull()), 1),
        )
        .select(group_by_column, result_pcr_logic_flag_column_name)
    )

    return df_output.join(df, [group_by_column], "inner").withColumn(
        result_pcr_logic_flag_column_name,
        F.when(
            (F.col(pcr_result_column_name) == "void") & (F.col(result_pcr_logic_flag_column_name) == 1), 1
        ).otherwise(None),
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

    return (
        df.withColumn("Ranking", F.rank().over(window))
        .withColumn(time_difference_logic_flag_column_name, F.when(F.col("Ranking") != 1, 1).otherwise(None))
        .drop("Ranking")
        .orderBy(*ordering_columns)
    )
