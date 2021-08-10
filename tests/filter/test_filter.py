import pytest
from chispa import assert_df_equality
from cishouseholds.filter import filter_by_cq_diff


@pytest.fixture
def dummy_df(spark_session):
    return spark_session.createDataFrame(
                data = [
                    ("ONS10948406", "2021-08-13 07:09:41", 12.782375, "keep"),  # Example 1:
                    ("ONS10948406", "2021-08-13 07:09:41", 12.782370, "delete"),  # smaller than 10^-5 - DELETE
                    ("ONS10948406", "2021-08-13 07:09:41", 12.783275, "keep"),  # larger than 10^-5 - KEEP
                    ("ONS10948406", "2021-08-13 07:09:41", 12.983275, "keep"),  # larger than 10^-5 - KEEP
                    ("ONS74697669", "2021-08-17 07:09:41", 200.782375, "keep"),  # Example 2:
                    ("ONS74697669", "2021-08-17 07:09:41", 200.782370, "delete"),  # smaller than 10^-5 - DELETE
                    ("ONS74697669", "2021-08-17 07:09:41", 200.783275, "keep"),  # larger than 10^-5 - KEEP
                ],
                schema = ["sample", "date_tested", "cq_value", "keep_or_delete"]
            )

def test_filter_by_cq_diff(dummy_df):  # test funtion

    df_input = dummy_df.drop("keep_or_delete")
    expected_df = dummy_df.filter(dummy_df.keep_or_delete == "keep").drop("keep_or_delete")
    actual_df = filter_by_cq_diff(df_input, "cq_value", "date_tested")

    assert_df_equality(actual_df, expected_df)
