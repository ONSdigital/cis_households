from typing import Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from cishouseholds.merge import one_to_many_bloods_flag


def assign_date_interval_and_flag(
    df: DataFrame,
    column_name_inside_interval: str,
    column_name_time_interval: str,
    start_datetime_reference_column: str,
    end_datetime_reference_column: str,
    lower_interval: Union[int, float],
    upper_interval: Union[int, float],
    interval_format: str = "hours",
) -> DataFrame:
    """
    This function gives the time interval in either hours (by default) or days
    in a column by given two date columns and says whether it is inside and
    upper and lower interval. If the difference of dates is within the upper and
    lower time intervals, the function will output None and an integer 1 if the
    difference in dates are outside of those intervals.
    Parameters
    ----------
    df
    column_name_inside_interval
        Name of the column that returns whether the difference in dates are
        within the upper/lower limits if within, it will return None, if outside
        will return an integer 1.
    column_name_time_interval
        Name of the column that returns the difference between start and end
        date and adds at the end of the column name whether it is in hours or
        days
    start_datetime_reference_column
        Earliest date in string format yyyy-mm-dd hh:mm:ss.
    end_datetime_reference_column
        Latest date in string format yyyy-mm-dd hh:mm:ss.
    lower_interval
        Marks how much NEGATIVE time difference can have between
        end_datetime_reference_column and start_datetime_reference_column.
        Meaning how the end_datetime_reference_column can be earlier than
        start_datetime_reference_column
    upper_interval
        Marks how much POSITIVE time difference can have between
        end_datetime_reference_column and start_datetime_reference_column
    interval_format
        By default will be a string called 'hours' if upper and lower intervals
        are input as days, define interval_format to 'days'. These are the only
        two possible formats.
    Notes
    -----
    Lower_interval should be a negative value if start_datetime_reference_column
    is after end_datetime_reference_column.
    """
    # by default, Hours but if days, apply change factor:
    if interval_format == "hours":  # to convert hours to seconds
        conversion_factor = 3600  # 1h has 60s*60min seconds = 3600 seconds
    elif interval_format == "days":
        conversion_factor = 86400  # 1 day has 60s*60min*24h seconds = 86400 seconds

    column_name_time_interval = column_name_time_interval + "_" + interval_format

    # FORMULA: (end_datetime_reference_column - start_datetime_reference_column) in
    # seconds/conversion_factor in seconds
    df = df.withColumn(
        column_name_time_interval,
        (
            F.to_timestamp(F.col(end_datetime_reference_column)).cast("long")
            - F.to_timestamp(F.col(start_datetime_reference_column)).cast("long")
        )
        / conversion_factor,  # 1 day has 60s*60min*24h seconds = 86400 seconds
    )

    return df.withColumn(
        column_name_inside_interval,
        F.when(~F.col(column_name_time_interval).between(lower_interval, upper_interval), 1).otherwise(None),
    )


spark = SparkSession.builder.getOrCreate()

# if True: #def test_flag_out_of_date_range(spark_session):

schema_iq = """count_iqvia integer, visit_date string, barcode_iq string"""
data_iq = [
    (1, "2029-01-01", "ONS00000001"),
    (1, "2029-01-01", "ONS00000002"),
    (1, "2029-01-01", "ONS00000003"),
    (1, "2029-01-02", "ONS00000003"),
    (2, "2029-01-01", "ONS00000004"),
    (2, "2029-01-02", "ONS00000004"),
    (1, "2029-01-04", "ONS00000003"),
    (1, "2029-01-01", "ONS00000003"),
    (1, "2029-01-02", "ONS00000004"),
    (1, "2029-01-04", "ONS00000004"),
]
schema_ox = """barcode_mk string, received_ox_date string, count_blood integer"""
data_ox = [
    ("ONS00000001", "2029-01-02", 1),
    ("ONS00000002", "2029-01-02", 2),
    ("ONS00000002", "2029-01-04", 2),
    ("ONS00000003", "2029-01-01", 1),
    ("ONS00000004", "2029-01-02", 2),
    ("ONS00000004", "2029-01-05", 2),
    ("ONS00000002", "2029-01-04", 2),
    ("ONS00000003", "2029-01-01", 4),
    ("ONS00000004", "2029-01-02", 2),
    ("ONS00000004", "2029-01-05", 3),
]
df_iq = spark.createDataFrame(data_iq, schema=schema_iq)
df_ox = spark.createDataFrame(data_ox, schema=schema_ox)

df_iq.show()
df_ox.show()
# IQ - 1:m - bloods

df_mrg = df_iq.join(df_ox, df_iq.barcode_iq == df_ox.barcode_mk, "inner")
df_mrg.show()
# df_mrg = assign_date_interval_and_flag(
#    df_mrg, "outside_interval_flag", "diff_interval", "visit_date", "received_ox_date", -24, 48
# )
# df_mrg.show()
# df_mrg.withColumn('grouping_window', F.first()).show()
# new_df = merge_one_to_many_iqvia_blood(df_mrg)
new_df = one_to_many_bloods_flag(df_mrg, "iq_1_to_m_bloods", "barcode_iq")
new_df.show()
