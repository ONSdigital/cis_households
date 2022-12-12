from datetime import datetime
from typing import Dict

import pyspark.sql.functions as F
from chispa import assert_df_equality
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from cishouseholds.edit import conditionally_set_column_values


def test_conditionally_set_column_values(spark_session):
    schema = "id integer, covid_1_date string, covid_2_date string, covid_3_date string, covid_1_test string, covid_2_test string, covid_3_test string, think_had_covid string, suspected_covid string, known_covid string"
    df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "2022-11-18", "2022-11-18", "2022-11-18", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (2, "2021-11-17", "2021-11-17", "2021-11-17", "Yes", "No",  "No",  "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (3, "2019-11-17", "2019-11-17", "2019-11-17", "Yes", "No",  "Yes", "Yes", "Yes", "Yes"),  # on covid invalid date so unaffected
            (4, "2018-11-18", "2018-11-18", "2018-11-18", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),  # before covid invalid date so unaffected
            (5, "2019-06-12", "2019-06-12", "2019-06-12", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),  # before covid invalid date so unaffected
        ],
        # fmt: on
        schema=schema,
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            (1, "2022-11-18", "2022-11-18", "2022-11-18", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (2, "2021-11-17", "2021-11-17", "2021-11-17", "Yes", "No",  "No",  "Yes", "Yes", "Yes"),  # after covid invalid date so unaffected
            (3, "2019-11-17", "2019-11-17", "2019-11-17", "Yes", "No",  "Yes", "Yes", "Yes", "Yes"),  # on covid invalid date so unaffected
            (4, None,         None,         None,         None,  None,  None,  "No",  "No",  "No"),  # before covid invalid date so unaffected
            (5, None,         None,         None,         None,  None,  None,  "No",  "No",  "No"),  # before covid invalid date so unaffected
        ],
        # fmt: on
        schema=schema,
    )
    for col in df.columns:
        if "date" in col:
            df = df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))
            expected_df = expected_df.withColumn(col, F.to_timestamp(F.col(col), format="yyyy-MM-dd"))
    invalid_covid_date = "2019-11-17"
    conditions = {
        "case_1": ((F.col("covid_1_date").isNotNull()) & (F.col("covid_1_date") < invalid_covid_date)),
        "case_2": ((F.col("covid_2_date").isNotNull()) & (F.col("covid_2_date") < invalid_covid_date)),
        "case_3": ((F.col("covid_3_date").isNotNull()) & (F.col("covid_3_date") < invalid_covid_date)),
    }
    covid_cols_to_clean = {
        "case_1": {
            "covid_1_": None,
            "think_had_covid": "No",
        },
        "case_2": {
            "covid_2_": None,
            "suspected_covid": "No",
        },
        "case_3": {
            "covid_3_": None,
            "known_covid": "No",
        },
    }
    for condition in list(conditions.keys()):
        df = conditionally_set_column_values(df, conditions.get(condition), covid_cols_to_clean.get(condition))
    assert_df_equality(expected_df, df, ignore_row_order=False, ignore_column_order=False, ignore_nullable=True)
