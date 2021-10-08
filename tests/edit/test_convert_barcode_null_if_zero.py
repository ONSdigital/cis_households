import pytest
from chispa import assert_df_equality

from cishouseholds.edit import convert_barcode_null_if_zero


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[(None, 1), ("ONS123456768", 2), (None, 3)],
        schema="barcode string, id integer",
    )


@pytest.fixture
def input_df(spark_session):
    return spark_session.createDataFrame(
        data=[("ONS0000", 1), ("ONS123456768", 2), ("GBK00000000", 3)],
        schema="barcode string, id integer",
    )


def test_convert_barcode_null_if_zero(expected_df, input_df):
    output_df = convert_barcode_null_if_zero(input_df, "barcode")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
