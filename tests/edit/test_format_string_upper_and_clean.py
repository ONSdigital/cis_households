import pytest
from chispa import assert_df_equality

from cishouseholds.edit import format_string_upper_and_clean


@pytest.fixture
def input_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("       this is a   string          .   ", 1),
        ],
        schema="""ref string, id integer""",
    )


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            ("THIS IS A STRING", 1),
        ],
        schema="""ref string, id integer""",
    )


def test_format_string_upper_and_clean(expected_df, input_df):
    output_df = format_string_upper_and_clean(input_df, "ref")
    assert_df_equality(expected_df, output_df)
