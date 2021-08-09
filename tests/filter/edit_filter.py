import pytest
from chispa import assert_df_equality

from cishouseholds.filter import filter_by_cq_diff


@pytest.mark.parametrize(
    "expected_data",
    [
        ("ONS10948406", "2021-08-13 07:09:41", 12.782375, "keep"),  # Example 1:
        ("ONS10948406", "2021-08-13 07:09:41", 12.782370, "delete"),  # smaller than 10^-5 - DELETE
        ("ONS10948406", "2021-08-13 07:09:41", 12.783275, "keep"),  # larger than 10^-5 - KEEP
        ("ONS10948406", "2021-08-13 07:09:41", 12.983275, "keep"),  # larger than 10^-5 - KEEP
        ("ONS74697669", "2021-08-17 07:09:41", 200.782375, "keep"),  # Example 2:
        ("ONS74697669", "2021-08-17 07:09:41", 200.782370, "delete"),  # smaller than 10^-5 - DELETE
        ("ONS74697669", "2021-08-17 07:09:41", 200.783275, "keep"),  # larger than 10^-5 - KEEP
    ],
)
def test_filter_by_cq_diff(spark_session, expected_data, filter_by_cq_diff):  # test funtion

    df = spark_session.createDataFrame(
        data=expected_data, schema=["sample", "date_tested", "cq_value", "keep_or_delete"]
    )
    df_input = df.drop("keep_or_delete")
    expected_df = df.filter(df.keep_or_delete == "keep").drop("keep_or_delete")
    actual_df = filter_by_cq_diff(df_input)

    assert_df_equality(actual_df, expected_df)
