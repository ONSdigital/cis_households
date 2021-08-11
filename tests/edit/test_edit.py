import pytest
from chispa import assert_df_equality
from cishouseholds.edit import edit_swab_results_single


def test_edit_swab_results(spark_session):
    # create example data
    schema = 'gene_result_classification string, gene_result_value double, overall_result_classification string'
    input_df = spark_session.createDataFrame(
        data = [
                    ('Negative', 0.0, 'Positive'), # nothing should happen
                    ('Positive', 0.0, 'Positive'), # this row should be edited
                    ('Negative', 1.0, 'Positive'), # nothing should happen
                ],
        schema = schema
        )
    
    expected_df = spark_session.createDataFrame(
        data = [
                    ('Negative', 0.0, 'Positive'), # nothing should happen
                    ('Negative', 0.0, 'Positive'), # after editing
                    ('Negative', 1.0, 'Positive'), # nothing should happen
                ],
        schema = schema
        )
    
    actual_df = edit_swab_results_single(input_df, "gene_result_classification",
                                        "gene_result_value", 'overall_result_classification')
    assert_df_equality(actual_df, expected_df)