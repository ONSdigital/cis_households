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
    # create a true 1 0 boolean representation of this column to be used later
    df = df.withColumn(
        "1_to_m_bloods_bool_flag",
        F.when((F.col("identify_one_to_many_bloods_flag") == 1), 0).otherwise(1),
    )
    # create columns for row number and group number in new grouped df
    window = Window.partitionBy(group_by_column).orderBy(
        "identify_one_to_many_bloods_flag", "diff_interval_hours", "visit_date"
    )
    df = df.withColumn("row_num", F.row_number().over(window))
    df.show()

    dft = df.groupBy(group_by_column).count().withColumnRenamed(group_by_column, "b")
    dft = dft.withColumn("dummy", F.lit(1))
    dft.show()
    mini_window = Window.partitionBy("dummy").orderBy("b")
    dft = dft.withColumn("group", F.row_number().over(mini_window))
    dft.show()
    df = df.join(dft, dft.b == df.barcode_iq).drop("b", "dummy")
    df.show()
    dff = (
        df.select(F.col("group"), F.col("row_num"), F.col("diff_interval_hours"))
        .filter(F.col("row_num") == 1)
        .withColumnRenamed("group", "g")
        .withColumnRenamed("diff_interval_hours", "d1_ref")
        .drop("row_num")
    )
    dff.show()

    # adding first diff interval ref and flagging records with different diff interval to first
    df = df.join(dff, dff.g == df.group).drop("g")
    df.show()
    df = df.withColumn(
        "dr1",
        F.when(
            (
                (F.col("identify_one_to_many_bloods_flag") == 1)
                & (F.col("d1_ref") != F.col("diff_interval_hours"))
                & (F.col("row_num") != 1)
            ),
            1,
        ).otherwise(
            None
        ),  # drop rows after first where diff vs visit is
    )
    df.show()

    # check for consistent siemens, tdi data
    dfts = (
        df.where(df.identify_one_to_many_bloods_flag == 1)
        .groupBy("group", "siemens")
        .count()
        .withColumnRenamed("group", "g")
        .withColumnRenamed("siemens", "s")
        .withColumnRenamed("count", "cs")
    )
    dftt = (
        df.where(df.identify_one_to_many_bloods_flag == 1)
        .groupBy("group", "tdi")
        .count()
        .withColumnRenamed("tdi", "t")
        .withColumnRenamed("count", "ct")
    )
    dfj = dfts.join(dftt, (dfts.g == dftt.group)).drop("group")
    # siemens count
    dfts.show()
    # tdi count
    dftt.show()
    dfj.show()

    # adding drop flag 2 column to indicate inconsitent siemens or tdi data within group
    dfn = dft.join(dfj, (dfj.g == dft.group)).drop("dummy", "group", "count")
    dfn.show()

    df = df.join(
        dfn,
        (dfn.b == df.barcode_iq) & (dfn.g == df.group) & (dfn.s.eqNullSafe(df.siemens)) & (dfn.t.eqNullSafe(df.tdi)),
    ).orderBy("group", "row_num")
    df = df.withColumn(
        "dr2",
        F.when(
            (F.col("identify_one_to_many_bloods_flag") == 1)
            & ((F.col("ct") != F.col("count")) | (F.col("cs") != F.col("count"))),
            1,
        ).otherwise(None),
    ).drop("s", "t", "ct", "cs", "b", "g")
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
        "1_to_m_bloods_bool_flag",
        "group",
        "count",
        "row_num",
        "d1_ref",
        "dr1",
        "dr2",
    )
