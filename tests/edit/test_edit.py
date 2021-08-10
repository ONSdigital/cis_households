import pytest
from chispa import assert_df_equality
from cishouseholds.edit import edit_swab_results_single


# Create an example
@pytest.fixture
def dummy_df(spark_session):
    return spark_session.createDataFrame(
        data = [
        ('Negative', 0.0, 'Positive', 'keep'), # nothing should happen
        ('Positive', 0.0, 'Positive', 'before'), # this row should be edited
        ('Negative', 0.0, 'Positive', 'after'), # after editing
        ('Negative', 1.0, 'Positive', 'keep'), # nothing should happen
        ],
        schema = ['gene_result_classification', 'gene_result_value', 
                    'overall_result_classification', 'selection']
        )

def test_edit_swab_results(dummy_df):

    input_df = dummy_df.filter((dummy_df.selection == 'keep') | (dummy_df.selection == 'before')).drop('selection')
    expected_df = dummy_df.filter((dummy_df.selection == 'keep') | (dummy_df.selection == 'after')).drop('selection')

    actual_df = edit_swab_results_single(input_df, "gene_result_classification",
                                        "gene_result_value", 'overall_result_classification')
    assert_df_equality(actual_df, expected_df)