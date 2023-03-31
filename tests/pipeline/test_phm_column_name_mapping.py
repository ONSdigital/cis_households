import pytest

from cishouseholds.pipeline.mapping import column_name_maps
from cishouseholds.pipeline.validation_schema import validation_schemas


@pytest.mark.regression
def test_phm_schema__and_json_match(spark_session):
    phm_column_name_map = column_name_maps["phm_column_name_map"]
    phm_validation_schema = validation_schemas["phm_survey_validation_schema"]

    phm_column_name_values = phm_column_name_map.values()
    phm_validation_schema_keys = phm_validation_schema.keys()

    if (set(phm_validation_schema_keys) - set(phm_column_name_values)) != set():
        error = (
            f"Expected:     {phm_validation_schema_keys}\n"
            f"Actual:       {phm_column_name_values}\n"
            f"Missing:      {set(phm_validation_schema_keys) - set(phm_column_name_values)}\n"
            f"Additional:   {set(phm_column_name_values) - set(phm_validation_schema_keys)}\n"
        )
        # print(error) # functional
        raise ValueError(error)
