import pytest
from chispa import assert_df_equality

from cishouseholds.derive import assign_ethnicity_white


@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data=[(1, "white"), (0, "non-white"), (1, "white")],
        schema="""white_bool integer, ethnicity_white string""",
    )


def test_assign_ethnicity_white(expected_df):
    output_df = assign_ethnicity_white(expected_df.drop("ethnicity_white"), "white_bool", "ethnicity_white")
    assert_df_equality(expected_df, output_df, ignore_nullable=True)
