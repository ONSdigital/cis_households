from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.merge import merge_assayed_bloods


def test_merge_assayed_bloods(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("S", "ONS00000002", 1, 111, 1, 2, 2, 2),  # singular
            ("N", "ONS00000003", 2, 112, 2, 1, 1, 1),  # match N and S
            ("S", "ONS00000003", 2, 112, 1, 2, 2, 1),
            ("S", "ONS00000004", 4, 113, 2, 1, 1, 1),
            ("N", "ONS00000004", 4, 114, 2, 1, 1, 1),  # mismatched well_id
        ],
        schema="""blood_group string, blood_sample_barcode string, antibody_test_plate_number integer,
                antibody_test_well_id integer, col1 integer, col2 integer, col3 integer, col4 integer""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("ONS00000004", 4, 114, None, None, None, None, None, "N", 2, 1, 1, 1),
            ("ONS00000002", 1, 111, "S", 1, 2, 2, 2, None, None, None, None, None),
            ("ONS00000004", 4, 113, "S", 2, 1, 1, 1, None, None, None, None, None),
            ("ONS00000003", 2, 112, "S", 1, 2, 2, 1, "N", 2, 1, 1, 1),
        ],
        schema="""blood_sample_barcode string ,antibody_test_plate_number integer,antibody_test_well_id integer,
        blood_group_s string,col1_s integer,col2_s integer,col3_s integer,col4_s integer,
        blood_group_n string,col1_n integer,col2_n integer,col3_n integer,col4_n integer""",
    )
    output_df = merge_assayed_bloods(input_df, "blood_group")
    assert_df_equality(expected_df, output_df)
