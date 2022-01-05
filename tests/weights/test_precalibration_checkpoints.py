from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import precalibration_checkpoints


def test_precalibration_checkpoints(spark_session):
    schema = """
                number_of_households_population_by_cis double,
                scaled_design_weight_adjusted_swab double,
                dweight_1 double,
                dweight_2 double,
                not_positive_or_null integer
            """
    expected_df_not_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
                (2.0,     1.0,   2.5,    1.0,    1),
                (2.0,     1.0,   -1.5,   1.2,    None),
                (2.0,     1.0,   -1.5,   None,   None),
            # fmt: on
        ],
        schema=schema,
    )

    input_df_not_pass = expected_df_not_pass.drop("not_positive_or_null")

    check_1, check_2_3, check_4 = precalibration_checkpoints(
        df=input_df_not_pass,
        test_type="swab",
        dweight_list=["dweight_1", "dweight_2"],
    )
    assert check_1 is not True
    assert check_2_3 is not True
    assert check_4 is True

    expected_df_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
                (3.0,     1.0,   2.5,   1.0,   None),
                (3.0,     1.0,   1.5,   1.2,   None),
                (3.0,     1.0,   1.5,   1.7,   None),
            # fmt: on
        ],
        schema=schema,
    )

    input_df_pass = expected_df_pass.drop("not_positive_or_null")

    check_1, check_2_3, check_4 = precalibration_checkpoints(
        df=input_df_pass,
        test_type="swab",
        dweight_list=["dweight_1", "dweight_2"],
    )
    assert check_1 is True
    assert check_2_3 is True
    assert check_4 is True
