from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pipeline.pipeline_stages import register_pipeline_stage


def test_register_pipeline_stage():
    """Test that registering a pipeline stage makes them available for running in the pipeline."""

    @register_pipeline_stage("a_test_ETL_reference")
    def a_test_ETL():
        pass

    assert pipeline_stages["a_test_ETL_reference"] == a_test_ETL
