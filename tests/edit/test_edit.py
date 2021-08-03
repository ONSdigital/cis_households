import pytest
from chispa import assert_df_equality
from cishouseholds.edit import edit_swab_results_single


# Create an example
@pytest.fixture
def expected_df(spark_session):
    return spark_session.createDataFrame(
        data = [(0, 0, 1),
                (0, 0, 1),
                (0, 1, 1)],
        schema = ["ctNgene_result","ctNgene","result_mk"]
    )


# example df:
def input_generator(spark_session):
    return spark_session.createDataFrame(
            data = [(0, 0, 1),
                    (1, 0, 1),
                    (0, 1, 1)],
            schema = ["ctNgene_result","ctNgene","result_mk"])


# Test
def test_edit_swab_results(spark_session, expected_df):
    df_input = input_generator(spark_session)
    actual_df = edit_swab_results_single(df_input, "ctNgene")
    assert_df_equality(actual_df, expected_df)