import pytest
from chispa import assert_df_equality

from cishouseholds.merge import one_to_many_antibody_flag


@pytest.mark.xfail(reason="units do not function correctly")
def test_one_to_many_antibody_flag(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt:off
                ('a',	2, "2029-01-01", "ONS00000003", "negative",     "ONS00000003", "2029-01-01", 2, "positive", 0.0,    None,   None),
                ('b',	2, "2029-01-01", "ONS00000003", "positive",     "ONS00000003", "2029-01-01", 2, "positive", 0.0,    None,   None),
                ('c',	2, "2029-01-06", "ONS00000005", "positive",     "ONS00000005", "2029-01-05", 2, "positive", -24.0,  None,   1),
                ('d',	2, "2029-01-06", "ONS00000005", "positive",     "ONS00000005", "2029-01-06",2, "positive",0.0,      None,   None),
                ('e',	2,"2029-01-04", "ONS00000007", "positive",      "ONS00000007", "2029-01-05",2, "positive",24.0,     None,   None),
                ('f',	2,"2029-01-04", "ONS00000007", "positive",      "ONS00000007", "2029-01-05",2, "positive",24.0,     None,   1),
                ('g',	1,"2029-01-04", "ONS00000006", "negative",      "ONS00000006", "2029-01-05",1, "negative",24.0,     None,   None),
                ('h',	6,"2029-01-02", "ONS00000004", "negative",      "ONS00000004", "2029-01-02",6, "negative",0.0,      None,   None),
                ('i',	6, "2029-01-02", "ONS00000004", "negative",     "ONS00000004", "2029-01-05", 6, None, 72.0,         1,      1),
                ('j',	6,"2029-01-02", "ONS00000004", "negative",      "ONS00000004", "2029-01-02",6, "negative",0.0,      None,   1),
                ('k',	6, "2029-01-01", "ONS00000004", "negative",     "ONS00000004", "2029-01-02", 6, "negative", 24.0,   None,   1),
                ('l',	6, "2029-01-02", "ONS00000004", "negative",     "ONS00000004", "2029-01-03", 6, None, 24.0,         None,   1),
                ('m',	6, "2029-01-01", "ONS00000004", "negative",     "ONS00000004", "2029-01-03", 6, None, 48.0,         None,   1),
            # fmt: on
        ],
        schema="""
            unique_antibody_test_id string,
            count_barcode_voyager integer,
            visit_date string,
            barcode_iq string,
            tdi string,
            barcode_ox string,
            received_ox_date string,
            count_barcode_antibody integer,
            siemens string,
            diff_interval_hours double,
            out_of_date_range_antibody integer,
            1tom_antibody_drop_flag integer
            """,
    )

    input_df = expected_df.drop(
        "identify_1tom_antibody_flag", "1tom_antibody_drop_flag", "failed_due_to_indistinct_match"
    )
    output_df = one_to_many_antibody_flag(
        df=input_df,
        column_name_to_assign="1tom_antibody_drop_flag",
        group_by_column="barcode_iq",
        diff_interval_hours="diff_interval_hours",
        siemens_column="siemens",
        tdi_column="tdi",
        visit_date="visit_date",
        # out_of_date_range_column="out_of_date_range_antibody",
        # count_barcode_voyager_column_name="count_barcode_voyager",
        # count_barcode_labs_column_name="count_barcode_antibody",
    )

    output_df = output_df.drop("failed_flag_1tom_antibody")
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
