from chispa import assert_df_equality

from cishouseholds.weights import household_design_weights


def test_merge_one_to_many_swab(spark_session):
    # address base file ---------------
    schema_address_base = """uprn integer,
                            postcode string"""
    data_address_base = [
        (1, "G68 9FG"),
        (2, "G68 9FH"),
        (3, "EC2V 7QJ"),
        (4, "EC1A 7BL"),
        (4, "EC1A 7BL"),  # 2 counts of uprn **
    ]
    df_input_address_base = spark_session.createDataFrame(data_address_base, schema=schema_address_base)

    # NSPL ---------------
    schema_nspl = """pcd string,
                    lsoa11 string"""
    data_nspl = [
        ("G68 9FZ", "S01010260"),
        ("G1 1BA", "S01010260"),
        ("G1 1BL", "S01010263"),
        ("G1 1BP", "S01010263"),
        ("EC1A 7BL", "S01010265"),  # postcode match
        ("EC2V 7QJ", "S01010260"),  # postcode match
        ("G68 9FH", "S01010260"),  # postcode match
        ("G68 9FG", "S01010265"),  # postcode match
    ]
    df_input_nspl = spark_session.createDataFrame(data_nspl, schema=schema_nspl)

    # LSOA to CIS area lookup ---------------
    schema_lsoa = """lsoa11cd string,
                    cis20cd string,
                    interim_id integer"""
    data_lsoa = [
        ("E01000001", "J06000173", 73),
        ("E01000002", "J06000173", 73),
        ("S01010263", "J06000173", 73),
        ("S01010260", "J06000172", 72),  # match
        ("S01010265", "J06000172", 72),  # match
    ]
    df_input_lsoa = spark_session.createDataFrame(data_lsoa, schema=schema_lsoa)

    schema_expected_aftgroup = """nb_address integer,
                                cis20cd string"""
    data_expected_aftgroup = [
        (1, "J06000172"),
        (1, "J06000172"),
        (1, "J06000172"),
        (2, "J06000172"),
    ]
    df_expected = spark_session.createDataFrame(data_expected_aftgroup, schema=schema_expected_aftgroup)

    df_output = household_design_weights(df_input_address_base, df_input_nspl, df_input_lsoa)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True)
