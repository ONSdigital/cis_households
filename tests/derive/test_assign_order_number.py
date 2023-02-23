from chispa import assert_df_equality

from cishouseholds.derive import assign_order_number


def test_assign_order_number(spark_session):
    expected_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            (11,    "Moderna",                                  3,  "No",  "Yes",   1), # Matches condition 1
            (21,    "Pfizer/BioNTech",                          2,  "Yes", "No",    1), # Matches condition 2
            (22,    "Oxford/AstraZeneca",                       1,  "Yes", "No",    1), # Matches condition 2
            (31,    "Bivalent",                                 3,  "No",  "Yes",   2), # Matches condition 3
            (41,    "Oxford/AstraZeneca",                       3,  "Yes", "Yes",   2), # Matches condition 4
            (51,    "Oxford/AstraZeneca",                       3,  "No",  "Yes",   3), # Matches condition 5
            (61,    "Moderna / Spikevax (including bivalent)",  3,  "Yes", "Yes",   3), # Matches condition 6
            (71,    "Sputnik",                                  3,  "Yes", "Yes",   4), # Matches condition 7
            (81,    "From a research study/trial",              1,  "Yes", "No",    5), # Matches condition 8
            (82,    "Sputnik",                                  1,  "Yes", "No",    5), # Matches condition 8
            (83,    "Sinovax",                                  1,  "Yes", "No",    5), # Matches condition 8
        ],
        # fmt: on
        schema="id integer, cis_covid_vaccine_type string, cis_covid_vaccine_number_of_doses int, pos_1_2 string, max_doses string, order_number int",
    )
    output_df = assign_order_number(
        df=expected_df.drop("order_number"),
        column_name_to_assign="order_number",
        covid_vaccine_type_column="cis_covid_vaccine_type",
        num_doses_column="cis_covid_vaccine_number_of_doses",
        max_doses_column="max_doses",
        pos_1_2_column="pos_1_2",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True)
