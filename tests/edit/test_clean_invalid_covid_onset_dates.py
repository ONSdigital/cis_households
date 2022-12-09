from datetime import datetime

import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from cishouseholds.edit import clean_invalid_covid_onset_dates


def test_clean_invalid_covid_onset_dates(spark_session):
    schema = "id integer, think_had_covid_onset_date string, covid_test_1 string, covid_test_2 string, covid_test_3 string, think_had_covid string"
    input_df = spark_session.createDataFrame(
        data=[  # fmt:off
            (1, "2022-11-18", "Yes", "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (2, "2021-11-17", "Yes", "No", "No", "Yes"),  # after covid invalid date so unaffected
            (3, "2019-11-17", "Yes", "No", "Yes", "Yes"),  # on covid invalid date so unaffected
            (4, "2018-11-18", "Yes", "Yes", "Yes", "Yes"),  # before covid invalid date so unaffected
            (5, "2019-06-12", "Yes", "Yes", "Yes", "Yes"),  # before covid invalid date so unaffected
        ],  # fmt:on
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[  # fmt:off
            (4, None, None, None, None, "No"),  # before covid invalid date so unaffected
            (5, None, None, None, None, "No"),  # before covid invalid date so unaffected
            (1, "2022-11-18", "Yes", "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (2, "2021-11-17", "Yes", "No", "No", "Yes"),  # after covid invalid date so unaffected
            (3, "2019-11-17", "Yes", "No", "Yes", "Yes"),  # on covid invalid date so unaffected
        ],  # fmt:on
        schema=schema,
    )
    for col in input_df.columns:
        if "date" in col:
            input_df = input_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))
            expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))

    covid_cols_to_clean = [
        "covid_test_1",
        "covid_test_2",
        "covid_test_3",
        "think_had_covid_onset_date",
    ]
    output_df = clean_invalid_covid_onset_dates(input_df, "2019-11-17", covid_cols_to_clean)
    assert_df_equality(expected_df, output_df, ignore_row_order=False, ignore_column_order=False, ignore_nullable=True)
