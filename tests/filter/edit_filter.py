import pytest
from chispa import assert_df_equality
from cishouseholds.filter import filter_by_cq_diff

# test data
@pytest.mark.parametrize(
    "expected_data",
    [
        ('ONS10948406', '2021-08-13 07:09:41', 12.782375), # Example 1:
        ('ONS10948406', '2021-08-13 07:09:41', 12.783275), # larger than 10^-5 - KEEP
        ('ONS10948406', '2021-08-13 07:09:41', 12.983275), # larger than 10^-5 - KEEP
        
        ('ONS74697669', '2021-08-17 07:09:41', 200.782375), # Example 2:
        ('ONS74697669', '2021-08-17 07:09:41', 200.783275), # larger than 10^-5 - KEEP
    ])
# test funtion
def test_filter_by_cq_diff(spark_session, expected_data):
    expected_schema = "sample string, date_tested string, cq_value double"
    expected_df = (
        spark_session.createDataFrame(
            [expected_data],
            schema=expected_schema
        )
    )
    # input dataframe
    df_input = spark_session.createDataFrame(
        data = [('ONS10948406', '2021-08-13 07:09:41', 12.782375), # Example 1:
            ('ONS10948406', '2021-08-13 07:09:41', 12.782370), # smaller than 10^-5 - DELETE
            ('ONS10948406', '2021-08-13 07:09:41', 12.783275), # larger than 10^-5 - KEEP
            ('ONS10948406', '2021-08-13 07:09:41', 12.983275), # larger than 10^-5 - KEEP

            ('ONS74697669', '2021-08-17 07:09:41', 200.782375), # Example 2:
            ('ONS74697669', '2021-08-17 07:09:41', 200.782370), # smaller than 10^-5 - DELETE
            ('ONS74697669', '2021-08-17 07:09:41', 200.783275), # larger than 10^-5 - KEEP
        ], 
        schema = "sample string, date_tested string, cq_value double"
        )

    actual_df = filter_by_cq_diff(df_input, 'cq_value', 'date_tested')
    assert_df_equality(actual_df, expected_df)
