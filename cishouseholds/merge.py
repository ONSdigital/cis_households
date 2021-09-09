from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def join_dataframes(df1: DataFrame, df2: DataFrame, reference_column: str, join_type: str = "outer"):
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
    return df1.join(df2, on=reference_column, how=join_type)


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
    df.show()
    return df, dft


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
        .withColumnRenamed(reference_column, "{}{}_ref".format(reference_column, row_num))
        .drop(row_column)
    )
    df = df.join(dft, dft.g == F.col(group_column)).drop("g")
    df.show()
    return df, "{}{}_ref".format(reference_column, row_num)


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
    df.show()
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
    dfj.show()
    return dfj


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
    df.show()
    return df


def one_to_many_bloods_flag(df: DataFrame, column_name_to_assign: str, group_by_column: str):
    # create a boolean column to flag whether or not a 1 to many match exists
    # between 1 iqvia and many bloods records
    df = assign_merge_process_group_flag(
        df,
        "identify_one_to_many_bloods_flag",
        "out_of_date_range_blood",
        "count_barcode_voyager",
        "==1",
        "count_barcode_blood",
        ">1",
    )
    df.show()
    # create columns for row number and group number in new grouped df
    window = Window.partitionBy(group_by_column).orderBy(
        "identify_one_to_many_bloods_flag", "diff_interval_hours", "visit_date"
    )
    df, dft = assign_group_and_row_number_columns(df, window, group_by_column)

    # adding first diff interval ref and flagging records with different diff interval to first

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

    # check for consistent siemens, tdi data
    dfj = check_consistent_data(df=df, check_column1="siemens", check_column2="tdi", group_by_column="group")

    # adding drop flag 2 column to indicate inconsitent siemens or tdi data within group
    dfn = dft.join(dfj, (dfj.g == dft.group)).drop("dummy", "group", "count")
    dfn.show()

    df = df.join(
        dfn,
        (dfn.b == df.barcode_iq) & (dfn.g == df.group) & (dfn.s.eqNullSafe(df.siemens)) & (dfn.t.eqNullSafe(df.tdi)),
    ).orderBy("group", "row_num")

    df = create_inconsistent_data_drop_flag(
        df=df, selection_column="identify_one_to_many_bloods_flag", item1_count_column="cs", item2_count_column="ct"
    ).drop("s", "t", "b", "g")
    df.show()

    # dropping extra columns after validation success
    df = df.withColumn(
        "one_to_many_bloods_drop_flag",
        F.when(
            (
                (F.col("identify_one_to_many_bloods_flag") == 1)
                & (((F.col("dr2") == 1) & (F.col("row_num") != 1)) | (F.col("dr1") == 1))
            ),
            1,
        ).otherwise(
            None
        ),  # drop rows after first where diff vs visit is
    )
    df.show()

    return df.drop(
        "out_of_date_range_blood",
        "identify_one_to_many_bloods_flag",
        "group",
        "count",
        "row_num",
        "d1_ref",
        "dr1",
        "dr2",
    )
