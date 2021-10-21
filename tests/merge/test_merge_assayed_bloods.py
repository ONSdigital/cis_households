from chispa.dataframe_comparer import assert_df_equality

from cishouseholds.merge import merge_assayed_bloods


def test_merge_assayed_bloods(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("S", "ONS00000002", 1, 1, None, None, None, None),
            ("N", "ONS00000003", None, None, None, 1, None, 1),
            ("N", "ONS00000003", None, None, None, 1, None, 1),
            ("S", "ONS00000004", None, None, None, 1, 1, 1),
            ("S", "ONS00000004", None, None, None, 1, 1, 1),  # check failed flags cannot apply to dropped columns
            ("S", "ONS00000005", 1, None, 1, None, None, None),
            ("N", "ONS00000005", 1, None, 1, None, None, None),
            ("S", "ONS00000005", 1, None, 1, None, None, None),  # check correct failed flags detected
            ("N", "ONS00000006", None, None, None, None, None, None),
            ("N", "ONS00000006", None, None, None, None, None, None),  # check a match has been applied
            ("S", "ONS00000007", 1, None, 1, 1, None, 1),  # check cannot flag as more than 1 type
        ],
        schema="""blood_group string, blood_sample_barcode string, antibody_test_plate_number integer,
                antibody_test_well_id integer, col1 integer, col2 integer, col3 integer, col4 integer""",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            ("ONS00000005", 1, None, "S", 1, None, None, None, None, None, None, None, None),
            ("ONS00000005", 1, None, "S", 1, None, None, None, None, None, None, None, None),
            ("ONS00000005", 1, None, None, None, None, None, None, "N", 1, None, None, None),
            ("ONS00000003", None, None, None, None, None, None, None, "N", None, 1, None, 1),
            ("ONS00000003", None, None, None, None, None, None, None, "N", None, 1, None, 1),
            ("ONS00000004", None, None, "S", None, 1, 1, 1, None, None, None, None, None),
            ("ONS00000004", None, None, "S", None, 1, 1, 1, None, None, None, None, None),
            ("ONS00000006", None, None, None, None, None, None, None, "N", None, None, None, None),
            ("ONS00000006", None, None, None, None, None, None, None, "N", None, None, None, None),
            ("ONS00000007", 1, None, "S", 1, 1, None, 1, None, None, None, None, None),
            ("ONS00000002", 1, 1, "S", None, None, None, None, None, None, None, None, None),
        ],
        schema="""blood_sample_barcode string ,antibody_test_plate_number integer,antibody_test_well_id integer,
        blood_group_s string,col1_s integer,col2_s integer,col3_s integer,col4_s integer,
        blood_group_n string,col1_n integer,col2_n integer,col3_n integer,col4_n integer""",
    )
    output_df = merge_assayed_bloods(input_df, "blood_group")
    assert_df_equality(expected_df, output_df)

    # comment
