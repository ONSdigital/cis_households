import pytest
from chispa import assert_df_equality
from cishouseholds.edit import edit_swab_results_single


# Create an example
@pytest.mark.parametrize(
    'input_data',
    [
        ('Negative', 0.0, 'Positive', 'keep'), # nothing should happen
        ('Positive', 0.0, 'Positive', 'before'), # this row should be edited
        ('Negative', 0.0, 'Positive', 'after'), # after editing
        ('Negative', 1.0, 'Positive', 'keep'), # nothing should happen
    ]
)

def test_edit_swab_results(spark_session, input_data):
    df = spark_session.createDataFrame(
        data = input_data,
        schema = ['gene_result_classification', 'gene_result_value', 
                    'overall_result_classification', 'selection']
    )
    
    input_df = df.filter((df.selection == 'keep') | (df.selection == 'before')).drop('selection')
    expected_df = df.filter((df.selection == 'keep') | (df.selection == 'after')).drop('selection')

    actual_df = edit_swab_results_single(input_df, "gene_result_classification",
                                        "gene_result_value", 'overall_result_classification')
    assert_df_equality(actual_df, expected_df)