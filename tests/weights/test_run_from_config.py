from chispa import assert_df_equality

from cishouseholds.weights.population_projections import run_from_config


def test_run_from_config(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 7, 1, 3, "England", 7),
            (2, 20, 2, 2, "England", 35),
            (3, 30, 3, 1, "Wales", 206),
        ],
        schema="interim_region_code integer, \
                interim_sex integer, \
                age_group_swab integer, \
                age_group_antibody integer, \
                country string,\
                p1_for_swab_longcovid integer",
    )
    input_df = expected_df.drop("p1_for_swab_longcovid")

    output_df = run_from_config(
        df=input_df, config_location="C:\code\cis_households\cishouseholds\weights\weights_config.yaml"
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
