from chispa import assert_df_equality

from cishouseholds.derive import assign_order_number


def test_assign_order_number(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (11,    "Moderna",                                          "No",  "Yes",   1), # Matches condition 1
            (21,    "Pfizer/BioNTech",                                  "Yes", "No",    1), # Matches condition 2
            (22,    "Oxford/AstraZeneca",                               "Yes", "No",    1), # Matches condition 2
            (31,    "Bivalent",                                         "No",  "Yes",   2), # Matches condition 3
            (41,    "Oxford/AstraZeneca",                               "Yes", "Yes",   2), # Matches condition 4
            (51,    "Oxford/AstraZeneca",                               "No",  "No",    3), # Matches condition 5
            (61,    "Moderna / Spikevax (including bivalent)",          "Yes", "Yes",   3), # Matches condition 6
            (71,    "Don't know type",                                  "Yes", "Yes",   4), # Matches condition 7
            (81,    "From a research study/trial",                      "Yes", "No",    5), # Matches condition 8
            (82,    "Sputnik",                                          "Yes", "No",    5), # Matches condition 8
            (83,    "Sinovax",                                          "Yes", "No",    5), # Matches condition 8
        ],
        # fmt: on
        schema="id integer, cis_covid_vaccine_type string, pos_1_2 string, max_doses string, order_number int",
    )
    output_df = assign_order_number(
        df=expected_df.drop("order_number"),
        column_name_to_assign="order_number",
        covid_vaccine_type_column="cis_covid_vaccine_type",
        max_doses_column="max_doses",
        pos_1_2_column="pos_1_2",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
