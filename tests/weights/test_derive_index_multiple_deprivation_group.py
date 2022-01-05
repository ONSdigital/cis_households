from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import derive_index_multiple_deprivation_group


def test_derive_index_multiple_deprivation_group(spark_session):
    schema_expected = """country_name_12 string,
                        index_multiple_deprivation integer,
                        index_multiple_deprivation_group integer"""
    data_expected_df = [
        # fmt: off
        ("England", 6569,   1),
        ("engLAND", 15607,  3),
        ("england", 57579,  5),
        ("WALES",   383,    2),
        ("wales",   1042,   3),
        ("Wales",   1358,   4),
        ("Northern Ireland",   160,    1),
        ("NORTHERN ireland",   296,    2),
        ("northern ireland",   823,   5),
        # fmt: on
    ]
    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected)
    df_input = df_expected.drop("index_multiple_deprivation_group")

    df_output = derive_index_multiple_deprivation_group(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
