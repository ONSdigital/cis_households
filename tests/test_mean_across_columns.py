from cishouseholds.derive import mean_across_columns
import pytest
from chispa import assert_df_equality

def test_mean(spark_session):
    expected_df = spark_session.createDataFrame([(1,2,3,2.0)], schema = 'col1 integer, col2 integer, col3 integer, mean double')
    input_df = expected_df.drop("mean")
    actual_df = mean_across_columns(input_df, 'mean', input_df.columns)
    assert_df_equality(actual_df, expected_df)
    