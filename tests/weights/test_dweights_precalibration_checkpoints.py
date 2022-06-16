import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from cishouseholds.weights.design_weights import DesignWeightError
from cishouseholds.weights.design_weights import validate_design_weights


@pytest.mark.xfail(reason="Input data do not pass checks")
def test_precal_and_design_weights_checkpoints(spark_session):
    schema = """
                id integer,
                country string,
                num_households integer,
                groupby double,
                sample_group string,
                design_weight_1 double,
                design_weight_2 double,
                not_positive_or_null integer
            """
    # test to NOT pass ------------------------
    expected_df_not_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
            #   id      country         num_households
            #                                   groupby     sample_group
            #                                                           design_weight_1
            #                                                                       design_weight_2
            #                                                                                   not_positive_or_null
                (1,     'england',      3,       2.0,       'new',      2.5,        1.0,        1),
                (1,     'england',      3,       2.0,       'new',      -1.5,       1.2,        None),
                (1,     'england',      3,       2.0,       'new',      -1.5,       None,       None),

                (1,     'england',      3,       2.0,       'old',      2.5,        1.0,        1),
                (1,     'england',      3,       2.0,       'old',      -1.5,       1.2,        None),
                (1,     'england',      3,       2.0,       'old',      -1.5,       None,       None),
            # fmt: on
        ],
        schema=schema,
    )
    input_df_not_pass = expected_df_not_pass.drop("not_positive_or_null")

    with pytest.raises(DesignWeightError):
        validate_design_weights(
            df=input_df_not_pass,
            num_households_by_cis_column="num_households",
            num_households_by_country_column="num_households",
            swab_weight_column="design_weight_1",
            antibody_weight_column="design_weight_2",
            cis_area_column="country",
            country_column="country",
            rounding_value=18,
        )

    # test to pass ------------------------
    expected_df_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
            #    id     country         num_households
            #                                   groupby     sample_group
            #                                                           design_weight_1
            #                                                                       design_weight_2
            #                                                                                   not_positive_or_null
                (1,     'england',      21,     3.0,        'new',      7.0,        7.0,        None),
                (1,     'england',      21,     3.0,        'new',      7.0,        7.0,        None),
                (1,     'england',      21,     3.0,        'new',      7.0,        7.0,        None),

                (1,     'england',      12,     3.0,        'old',      4.0,        4.0,        None),
                (1,     'england',      12,     3.0,        'old',      4.0,        4.0,        None),
                (1,     'england',      12,     3.0,        'old',      4.0,        4.0,        None),
            # fmt: on
        ],
        schema=schema,
    )
    for column in ["design_weight_1", "design_weight_2"]:
        expected_df_pass = expected_df_pass.withColumn(column, F.col(column).cast(DecimalType(38, 20)))

    input_df_pass = expected_df_pass.drop("not_positive_or_null")

    (
        swab_weight_column_type,
        antibody_weight_column_type,
        antibody_design_weights_sum_to_population,
        swab_design_weights_sum_to_population,
        check_negative_design_weights,
        check_null_design_weights,
    ) = validate_design_weights(
        df=input_df_pass,
        num_households_by_cis_column="num_households",
        num_households_by_country_column="num_households",
        swab_weight_column="design_weight_1",
        antibody_weight_column="design_weight_2",
        cis_area_column="country",
        country_column="country",
        swab_group_by_columns=["country", "groupby", "sample_group"],
    )

    assert swab_weight_column_type is True
    assert antibody_weight_column_type is True
    assert antibody_design_weights_sum_to_population is True
    assert swab_design_weights_sum_to_population is True
    assert check_negative_design_weights is not True
    assert check_null_design_weights is not True
