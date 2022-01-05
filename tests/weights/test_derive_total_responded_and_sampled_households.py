from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import derive_total_responded_and_sampled_households


def test_derive_total_responded_and_sampled_households(spark_session):

    schema_expected_df = """ons_household_id string,
                            sample_addressbase_indicator integer,
                            country_name_12 string,
                            cis_area_code_20 integer,
                            index_multiple_deprivation_group integer,
                            interim_participant_id integer,
                            total_sampled_households_cis_imd_addressbase integer,
                            total_responded_households_cis_imd_addressbase integer"""
    data_expected_df = [
        # fmt: off
            ("A1", 1, "england",            1, 1, 1, 5, 3),
            ("A2", 1, "england",            1, 1, 1, 5, 3),
            ("A3", 1, "england",            1, 1, 1, 5, 3),
            ("A4", 1, "england",            1, 1, 0, 5, 3),
            ("A5", 1, "england",            1, 1, 0, 5, 3),
            ("B1", 2, "NORTHERN IRELAND",   2, 2, 1, 4, 2),
            ("B2", 2, "Northern Ireland",   2, 2, 1, 4, 2),
            ("B3", 2, "NORThern irelAND",   2, 2, 0, 4, 2),
            ("B4", 2, "northern ireland",   2, 2, 0, 4, 2),
        # fmt: on
    ]

    df_expected = spark_session.createDataFrame(data_expected_df, schema=schema_expected_df)

    df_input = df_expected.drop(
        "total_sampled_households_cis_imd_addressbase", "total_responded_households_cis_imd_addressbase"
    )
    df_output = derive_total_responded_and_sampled_households(df_input)

    assert_df_equality(df_output, df_expected, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
