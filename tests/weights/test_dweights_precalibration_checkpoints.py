import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from cishouseholds.weights.design_weights import DesignWeightError
from cishouseholds.weights.design_weights import validate_design_weights


def test_precal_and_design_weights_checkpoints_fail(spark_session):
    expected_df_not_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
            #   id      country         num_households
            #                                   design_weight_1
            #                                                design_weight_2
            #                                                            not_positive_or_null
                (1,     'england',      3,       2.5,        1.0,        1),
                (1,     'england',      3,       -1.5,       1.2,        None),
                (1,     'england',      3,       -1.5,       None,       None),
            # fmt: on
        ],
        schema="""
                id integer,
                country string,
                num_households integer,
                design_weight_1 double,
                design_weight_2 double,
                not_positive_or_null integer
            """,
    )
    input_df_not_pass = expected_df_not_pass.drop("not_positive_or_null")

    with pytest.raises(DesignWeightError) as weightexception:
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

        assert all(
            [
                x in weightexception.value
                for x in [
                    "Antibody design weights do not sum to country population totals",
                    "Swab design weights do not sum to cis area population totals.",
                    "records have negative design weights.",
                    "records with null swab or antibody design weights.",
                ]
            ]
        )


def test_precal_and_design_weights_checkpoints_pass(spark_session):
    expected_df_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
            #    id     country         num_households
            #                                  design_weight_1
            #                                               design_weight_2
            #                                                           not_positive_or_null
                (1,     'england',      21,     7.0,        7.0,        None),
                (1,     'england',      21,     7.0,        7.0,        None),
                (1,     'england',      21,     7.0,        7.0,        None),
            # fmt: on
        ],
        schema="""
                id integer,
                country string,
                num_households integer,
                design_weight_1 double,
                design_weight_2 double,
                not_positive_or_null integer
            """,
    )
    for column in ["design_weight_1", "design_weight_2"]:
        expected_df_pass = expected_df_pass.withColumn(column, F.col(column).cast(DecimalType(38, 20)))

    input_df_pass = expected_df_pass.drop("not_positive_or_null")

    validate_design_weights(
        df=input_df_pass,
        num_households_by_cis_column="num_households",
        num_households_by_country_column="num_households",
        swab_weight_column="design_weight_1",
        antibody_weight_column="design_weight_2",
        cis_area_column="country",
        country_column="country",
    )
