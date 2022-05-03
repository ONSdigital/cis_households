import pytest

from cishouseholds.weights.pre_calibration import DesignWeightError
from cishouseholds.weights.weights import validate_design_weights_or_precal


def test_validate_design_weights(spark_session):
    input1_df = spark_session.createDataFrame(  # fails check 1 due to erroneous antibody weight
        data=[
            (1, 2.0, 2.0, 1.0, 1.0, 6.0),
            (1, 1.0, 2.0, 1.0, -2.0, 6.0),
            (1, 3.0, -2.0, 2.0, -1.0, 6.0),
            (1, -1.0, -1.0, 4.0, 3.0, 6.0),
            (1, None, None, -5.0, -1.0, 6.0),
            (1, 1.0, 5.0, 3.0, 2.0, 6.0),
        ],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            swab_weight double,
            antibody_weight double,
            num_hh double
            """,
    )
    input2_df = spark_session.createDataFrame(  # fails check 1 due to erroneous antibody weight
        data=[
            (1, 2.0, 2.0, 1.0, 1.0, 6.0),
            (1, 1.0, 2.0, 1.0, 2.0, 6.0),
            (1, 3.0, -2.0, 2.0, -1.0, 6.0),
            (1, -1.0, -1.0, 4.0, 3.0, 6.0),
            (1, None, None, -5.0, -1.0, 6.0),
            (1, 1.0, 5.0, 3.0, 2.0, 6.0),
        ],
        schema="""
            window integer,
            weight1 double,
            weight2 double,
            swab_weight double,
            antibody_weight double,
            num_hh double
            """,
    )
    with pytest.raises(DesignWeightError, match="check_1: The design weights are NOT adding up to total population."):
        output_df = validate_design_weights_or_precal(
            df=input1_df,
            num_households_column="num_hh",
            swab_weight_column="swab_weight",
            antibody_weight_column="antibody_weight",
            group_by_columns=["window"],
        )
    with pytest.raises(DesignWeightError, match="check_2: The design weights are NOT all are positive."):
        output_df = validate_design_weights_or_precal(
            df=input2_df,
            num_households_column="num_hh",
            swab_weight_column="swab_weight",
            antibody_weight_column="antibody_weight",
            group_by_columns=["window"],
        )
