from chispa import assert_df_equality

from cishouseholds.weights.design_weights import household_level_populations


def test_household_level_populations(spark_session):
    schema_address_base = """
            unique_property_reference_code integer,
            postcode string
        """
    data_address_base = [
        (1, "AAA BBB"),
        (2, "AAA AAA"),
        (3, "CCC EEE"),
        (4, "DDD FFF"),
        (5, "DDD GGG"),  # uprn unique to household **
    ]
    df_input_address_base = spark_session.createDataFrame(data_address_base, schema=schema_address_base)

    schema_nspl = """
        postcode string,
        lower_super_output_area_code_11 string,
        country_code_12 string
    """
    data_nspl = [
        ("DDD GGG", "E2", "C2"),
        ("HHH XXX", "E1", "C2"),
        ("HHH YYY", "S3", "C2"),
        ("HHH ZZZ", "S3", "C3"),
        ("DDD FFF", "S5", "C6"),  # postcode match from address base, country code not in lookup
        ("CCC EEE", "E2", "C2"),  # postcode match from address base
        ("AAA AAA", "S1", "C3"),  # postcode match from address base
        ("AAA BBB", "S4", "C2"),  # postcode match from address base, lsoa not in lookup
    ]
    df_input_nspl = spark_session.createDataFrame(data_nspl, schema=schema_nspl)

    schema_lsoa = """
        lower_super_output_area_code_11 string,
        cis_area_code_20 string,
        interim_id integer
    """
    data_lsoa = [
        ("E1", "J3", 73),
        ("E2", "J3", 73),
        ("S3", "J3", 73),
        ("S1", "J2", 72),  # match lsoa from nspl
        ("S5", "J2", 72),  # match lsoa from nspl
    ]
    df_input_lsoa = spark_session.createDataFrame(data_lsoa, schema=schema_lsoa)

    schema_country = """
        country_code_12 string,
        name string
    """
    data_country = [
        ("C1", "name1"),
        ("C2", "name2"),  # match country code from nspl
        ("C3", "name3"),  # match country code from nspl
        ("C4", "name4"),
        ("C5", "name5"),
    ]
    df_input_country = spark_session.createDataFrame(data_country, schema=schema_country)

    schema_expected = """
        country_code_12 string,
        lower_super_output_area_code_11 string,
        postcode string,
        unique_property_reference_code integer,
        cis_area_code_20 string,
        interim_id integer,
        name string,
        number_of_households_by_cis_area integer,
        number_of_households_by_country integer
    """
    data_expected = [
        ("C2", "E2", "DDD GGG", 5, "J3", 73, "name2", 2, 3),
        ("C6", "S5", "DDD FFF", 4, "J2", 72, None, 2, 1),
        ("C3", "S1", "AAA AAA", 2, "J2", 72, "name3", 2, 1),
        ("C2", "S4", "AAA BBB", 1, None, None, "name2", 1, 3),
        ("C2", "E2", "CCC EEE", 3, "J3", 73, "name2", 2, 3),
    ]
    df_expected = spark_session.createDataFrame(data_expected, schema=schema_expected)
    df_output = household_level_populations(df_input_address_base, df_input_nspl, df_input_lsoa, df_input_country)
    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
