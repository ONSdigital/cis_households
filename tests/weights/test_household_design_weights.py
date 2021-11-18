from chispa import assert_df_equality

from cishouseholds.design_weights import household_design_weights


def test_merge_one_to_many_swab(spark_session):
    # address base file ---------------
    schema_address_base = """uprn integer,
                            postcode string"""
    data_address_base = [
        (1, "A B"),
        (2, "A A"),
        (3, "C E"),
        (4, "D F"),
        (4, "D F"),  # 2 counts of uprn **
    ]
    df_input_address_base = spark_session.createDataFrame(data_address_base, schema=schema_address_base)

    # NSPL ---------------
    schema_nspl = """pcd string,
                    lsoa11 string"""
    data_nspl = [
        ("A T", "S1"),
        ("H X", "S1"),
        ("H Y", "S3"),
        ("H Z", "S3"),
        ("D F", "S5"),  # postcode match
        ("C E", "S1"),  # postcode match
        ("A A", "S1"),  # postcode match
        ("A B", "S5"),  # postcode match
    ]
    df_input_nspl = spark_session.createDataFrame(data_nspl, schema=schema_nspl)

    # LSOA to CIS area lookup ---------------
    schema_lsoa = """lsoa11cd string,
                    cis20cd string,
                    interim_id integer"""
    data_lsoa = [
        ("E1", "J3", 73),
        ("E2", "J3", 73),
        ("S3", "J3", 73),
        ("S1", "J2", 72),  # match
        ("S5", "J2", 72),  # match
    ]
    df_input_lsoa = spark_session.createDataFrame(data_lsoa, schema=schema_lsoa)

    schema_expected_aftgroup = """interim_id integer,
                                nb_address integer,
                                cis20cd string"""
    data_expected_aftgroup = [
        (72, 1, "J2"),
        (72, 1, "J2"),
        (72, 1, "J2"),
        (72, 2, "J2"),
    ]
    df_expected = spark_session.createDataFrame(data_expected_aftgroup, schema=schema_expected_aftgroup)
    df_output = household_design_weights(df_input_address_base, df_input_nspl, df_input_lsoa)
    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True)
