import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_taken_column


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[(None, "No"), ("ONS123456768", "Yes")],
        schema="barcode string, taken string",
    )


def test_assign_taken_column(expected_df):
    output_df = assign_taken_column(expected_df.drop("taken"), "taken", "barcode")
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
