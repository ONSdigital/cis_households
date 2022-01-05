from chispa import assert_df_equality

from cishouseholds.weights.population_projections import run_from_config


def test_run_from_config(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 7, 1, 3, "England", 7, 33, 33, None, "missing"),
            (2, 20, 2, 2, "England", 35, 107, 97, None, "missing"),
            (3, 30, 3, 1, "Wales", 206, None, None, 146, 146),
        ],
        schema="interim_region_code integer, \
                interim_sex integer, \
                age_group_swab integer, \
                age_group_antibody integer, \
                country string,\
                p1_for_swab_longcovid integer,\
                p1_for_antibodies_evernever_engl integer,\
                p1_for_antibodies_28daysto_engl integer,\
                p1_for_antibodies_wales_scot_ni integer,\
                p3_for_antibodies_28daysto_engl string",
    )
    input_df = expected_df.drop(
        "p1_for_swab_longcovid",
        "p1_for_antibodies_evernever_engl",
        "p1_for_antibodies_28daysto_engl integer",
        "p1_for_antibodies_wales_scot_ni integer",
        "p3_for_antibodies_28daysto_engl string",
    )

    output_df = run_from_config(
        df=input_df,
        config_location=r"C:\code\cis_households\cishouseholds\weights\weights_config.yaml",
        country_column="country",
        age_group_antibody_column="age_group_antibody",
    )
    assert_df_equality(expected_df, output_df, ignore_column_order=True, ignore_row_order=True)
