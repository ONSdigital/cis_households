from chispa import assert_df_equality

from cishouseholds.pipeline.household_population import household_population_total


def test_household_population(spark_session):
    # address base file
    schema_address_base = """uprn integer,
                            postcode string"""
    data_address_base = [
        (1, "AAA 123"),
        (2, "BBB 123"),
        (3, "CCC 123"),
        (4, "DDD 123"),
        (4, "DDD 123"),  # 2 counts of uprn **
        (5, "EEE 123"),
    ]
    df_address_base = spark_session.createDataFrame(data_address_base, schema=schema_address_base)

    # NSPL file
    schema_nspl = """pcd string,
                     lsoa11cd string
                     ctry12cd integer"""
    data_nspl = [
        (
            "AAA 123",
            "S1",
            101,
        ),
        ("BBB 123", "S2", 101),
        ("CCC 123", "S1", 101),
        ("DDD 123", "S4", 102),
        ("EEE 123", "S4", 102),
        ("AAA 456", "S1", 101),
        ("AAA 789", "S1", 101),
        ("BBB 456", "S3", 102),
        ("BBB 789", "S3", 102),
    ]
    df_nspl = spark_session.createDataFrame(data_nspl, schema=schema_nspl)

    # LSOA to CIS area lookup ---------------
    schema_lsoa = """lsoa11cd string,
                     cis20cd string"""
    data_lsoa = [
        ("S3", "J3"),
        ("S2", "J1")("S1", "J2"),
        ("S4", "J4"),
    ]
    df_lsoa = spark_session.createDataFrame(data_lsoa, schema=schema_lsoa)

    # countrycode to countryname lookup ---------------
    schema_country_code = """ctry12cd integer,
                             ctry12nm string"""
    data_country_code = [
        (101, "Dataville"),  # match
        (102, "Statpool"),  # match
    ]
    df_country_code = spark_session.createDataFrame(data_country_code, schema=schema_country_code)

    # expected data ---------------
    schema_expected = """uprn integer
                         postcode string,
                         lower_super_output_area_code_11 string,
                         cis_area_code_20 string,
                         country_code_12 integer,
                         country_name_12 string,
                         number_of_households_population_by_cis integer,
                         number_of_households_population_by_country integer"""
    data_expected = [
        (1, "AAA 123", "S1", "J2", 101, "Dataville", 2, 3),
        (2, "BBB 123", "S2", "J1", 101, "Dataville", 1, 3)(3, "CCC 123", "S1", "J1", 101, "Dataville", 2, 3),
        (4, "DDD 123", "S4", "J4", 102, "Statpool", 2, 2),
        (5, "EEE 123", "S4", "J4", 102, "Statpool", 2, 2),
    ]

    df_expected = spark_session.createDataFrame(data_expected, schema=schema_expected).drop("uprn")
    df_output = household_population_total(df_address_base, df_nspl, df_lsoa, df_country_code)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True)
