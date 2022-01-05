from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import calculate_non_response_factors


def test_calculate_non_response_factors(spark_session):
    # fmt: off
    schema_expected_df = """country_name_12 string,
                            total_sampled_households_cis_imd_addressbase integer,
                            total_responded_households_cis_imd_addressbase integer,
                            raw_non_response_factor double,
                            mean_raw_non_response_factor double,
                            scaled_non_response_factor double,
                            bounded_non_response_factor double"""
    data_expected_df = [

            # scaled_non_response has to be smaller than 0.5 or larger than 2.0 non-inclusive
            ("England", 20, 6, 3.3, 5.0, 0.7, None),
            ("England", 17, 12, 1.4, 5.0, 0.3, 0.6),
            ("England", 25, 19, 1.3, 5.0, 0.3, 0.6),
            ("England", 28, 2, 14.0, 5.0, 2.8, 1.8),
            ("Northern Ireland", 15, 10, 1.5, 3.2, 0.5, None),
            ("Northern Ireland", 11, 2, 5.5, 3.2, 1.7, None),
            ("Northern Ireland", 9, 7, 1.3, 3.2, 0.4, 0.6),
            ("Northern Ireland", 18, 4, 4.5, 3.2, 1.4, None),
    ]
    # fmt: on

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    df_input = df_expected.drop("raw_non_response_factor")

    df_output = calculate_non_response_factors(df=df_input, n_decimals=1)
    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
