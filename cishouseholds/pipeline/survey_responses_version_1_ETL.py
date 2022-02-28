from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.edit import update_column_values_from_map


def transform_survey_responses_version_1_delta(df: DataFrame) -> DataFrame:
    """
    Call functions to process input for iqvia version 1 survey deltas.
    """
    df = assign_column_uniform_value(df, "survey_response_dataset_major_version", 1)
    df = update_column_values_from_map(
        df=df,
        column="household_been_hospital_last_28_days",
        map={"No, someone else in my household has": "No I haven’t, but someone else in my household has"},
    )
    df = update_column_values_from_map(
        df=df,
        column="household_been_care_home_last_28_days",
        map={"No, someone else in my household has": "No I haven’t, but someone else in my household has"},
    )

    df = df.withColumn("work_status_v0", F.col("work_status_v1"))

    work_status_dict = {
        "Employed and currently working": "Employed",  # noqa: E501
        "Employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
        "Self-employed and currently not working": "Furloughed (temporarily not working)",  # noqa: E501
        "Retired": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
        "Looking for paid work and able to start": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
        "Not working and not looking for work": "Not working (unemployed, retired, long-term sick etc.)",  # noqa: E501
        "Child under 5y not attending child care": "Student",  # noqa: E501
        "Child under 5y attending child care": "Student",  # noqa: E501
        "5y and older in full-time education": "Student",  # noqa: E501
        "Self-employed and currently working": "Self-employed",  # noqa: E501
    }

    df = update_column_values_from_map(df=df, column="work_status_v0", map=work_status_dict)

    return df
