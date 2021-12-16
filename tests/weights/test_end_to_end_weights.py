from pathlib import Path

import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.weights.extract import read_csv_to_pyspark_df
from cishouseholds.weights.weights import generate_weights

# fmt: off
def test_end_to_end_weights(spark_session):
    # static lookups
    address_lookup = spark_session.createDataFrame(
        data=[
            (10000001, "town name1", "A", "Scotland", "S12000050", "FALSE", "hh", 1, 2000001, "N"),
            (10000002, "town name1", "B", "Scotland", "S12000050", "FALSE", "hh", 0, 2000002, "L"),
            (10000003, "town name2", "C", "Northern Ireland", "N09000001", "FALSE", "hh", 0, 2000003, "L"),
            (10000004, "town name2", "D", "Northern Ireland", "N09000001", "FALSE", "hh", 1, 2000004, "N"),
            (10000005, "town name2", "E", "Northern Ireland", "N09000001", "FALSE", "hh", 1, 2000005, "L"),
            (10000006, "town name3", "F", "Northern Ireland", "N09000001", "FALSE", "hh", 1, 2000006, "N"),
        ],
        schema="""
            uprn integer,
            town_name string,
            postcode string,
            ctry18nm string,
            la_code string,
            ew string,
            address_type string,
            council_tax string,
            udprn integer,
            address_base_postal string
            """,
    )
    cis_20_lookup = spark_session.createDataFrame(
        data=[
            ("S1", "City of London", "J1", "E12000007"),
            ("S2", "Isle of Anglesey", "J2", "W92000004"),
            ("S3", "Culter", "J3", "S902000003"),
            ("S4", "Stiles", "J4", "N92000002"),
            ("S5", "Another place", "J5", "N92000001"),
            ("S6", "Somewhere else", "J6", "S902000004"),
            ("S7", "My house", "J7", "E12000005"),
        ],
        schema="""
            LSOA11CD string,
            LSOA11NM string,
            CIS20CD string,
            RGN19CD string
            """,
    )
    nspl_lookup = spark_session.createDataFrame(
        data=[
            ("A", "E", "S1"),
            ("B", "N", "S7"),
            ("C", "N", "S2"),
            ("D", "E", "S3"),
            ("E", "N", "S4"),
            ("F", "N", "S5"),
            ("G", "N", "S6"),
            ("H", "E", "S3"),
        ],
        schema="""
            pcd string,
            ctry string,
            lsoa11 string
            """,
    )
    country_lookup = spark_session.createDataFrame(
        data=[
            ("E06000001", "Hartlepool", "E", "England"),
            ("N09000009", "Swindon", "N", "Northen Ireland"),
            ("W09000004", "Mid Ulster", "W", "Wales"),
            ("S09000003", "Belfast", "S", "Scotland"),
        ],
        schema="""
            LAD20CD string,
        	LAD20NM string,
            CTRY20CD string,
            CTRY20NM string
            """,
    )

    # test dfs
    old_sample_df = spark_session.createDataFrame(
        schema="""
            UAC integer,
            postcode string,
            lsoa_11 string,
            cis20cd string,
            ctry12 string,
            ctry_name12 string,
            tranche integer,
            sample string,
            sample_direct integer,
            date_sample_created  string,
            batch_number integer,
            file_name string,
            hh_dweight_swab double,
            hh_dweight_atb double,
            rgngor9d string,
            laua string,
            oa11oac11 string,
            msoa11 string,
            ru11ind string,
            imd integer
        """,
        data=[
            (1, "A", "S1", "J5", "W", "Wales", 2, "AB", 1, "15/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", 3.5, 3.5, "W99999999", "W06000015", "W00010121", "W02000398", "C1", 999),
            (2, "B", "S2", "J5", "N", "Wales", 2, "LFS", 0, "15/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", 3.5, 3.5, "W99999999", "W06000015", "W00010121", "W02000398", "C1", 999),
            (3, "C", "S2", "J2", "N", "Wales", 2, "AB", 1, "15/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", 6.0, 6.0, "W99999999", "W06000015", "W00010121", "W02000398", "C1", 999),
            (4, "D", "S4", "J2", "N", "Northern Ireland", 2, "LFS", 0, "15/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", 6.0, 6.0, "N99999999", "N09000003", "N00001155", "N99999999", None, 29),
            (5, "E", "S3", "J4", "E", "Northern Ireland", 2, "AB", 1, "15/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", 4.0, 4.0, "N99999999", "N09000003", "N00001155", "N99999999", None, 29),
            (6, "F", "S3", "J3", "E", "Scotland", 2, "LFS", 0, "15/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", 4.0, 4.0, "S99999999", "S12000033", "S00090381", "S02001236", 3, 6253),
            (7, "A", "S5", "J2", "N", "Scotland", 2, "AB", 1, "15/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", 4.0, 4.0, "S99999999", "S12000033", "S00090399", "S02001237", 3, 6715),
            (8, "C", "S1", "J3", "S", "England", 2, "LFS", 0, "15/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", 6.5, 6.5, "E12000008", "E07000091", "E00116842", "E02004794", "F1", 13619),
            (9, "B", "S5", "J1", "E", "England", 2, "AB", 1, "15/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", 6.0, 6.0, "E12000008", "E07000091", "E00116840", "E02004794", "D1", 31052),
        ]
    )
    new_sample_df = spark_session.createDataFrame(
        schema="""
            UAC integer,
            postcode string,
            lsoa_11 string,
            cis20cd string,
            ctry12 string,
            ctry_name12 string,
            sample string,
            sample_direct integer,
            date_sample_created  string,
            batch_number integer,
            file_name string,
            rgngor9d string,
            laua string,
            oa11oac11 string,
            msoa11 string,
            ru11ind string,
            imd integer
        """,
        data=[
            (1, "A", "S1", "J1", "E", "England", "LFS", 0, "14/11/2021", 2, "sample_direct_eng_wc15112021_4.csv", "E12000002", "E06000008", "E00063413", "E02002622", "C1", 3369),
            (2, "B", "S1", "J5", "E", "England", "AB", 1, "14/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", "E12000002", "E06000008", "E00063735", "E02002620", "C1", 7612),
            (3, "C", "S2", "J3", "E", "England", "AB", 1, "14/11/2021", 1, "sample_direct_eng_wc15112021_4.csv", "E12000002", "E06000008", "E00063401", "E02002621", "C1", 2666),
            (4, "D", "S2", "J3", "S", "Scotland", "LFS", 0, "14/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", "S99999999", "S12000034", "S00091321", "S02001296", 6, 5069),
            (5, "E", "S3", "J2", "N", "Scotland", "AB", 1, "14/11/2021", 2, "sample_direct_eng_wc15112021_4.csv",  "S99999999", "S12000034", "S00092401", "S02001296", 6, 5069),
            (6, "F", "S3", "J4", "N", "Scotland", "AB", 1, "14/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", "S99999999", "S12000034", "S00092401", "S02001296", 6, 5069),
            (7, "E", "S4", "J3", "W", "Northern Ireland", "LFS", 0, "14/11/2021", 1, "sample_direct_eng_wc15112021_4.csv", "N99999999", "N09000003", "N00001131", "N99999999", None, 49),
            (8, "D", "S5", "J1", "S", "Northern Ireland", "AB", 1, "14/11/2021", 3, "sample_direct_eng_wc15112021_4.csv", "N99999999", "N09000003", "N00001352", "N99999999", None, 25),
            (9, "C", "S5", "J1", "E", "Northern Ireland", "AB", 1, "14/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", "N99999999", "N09000003", "N00001352", "N99999999", None, 25),
            (10, "B", "S6", "J3", "N", "Wales", "AB", 1, "14/11/2021", 1, "sample_direct_eng_wc15112021_4.csv", "W99999999", "W06000015", "W00010141", "W02000398", "C1", 833),
            (11, "A", "S6", "J3", "E", "Wales", "AB", 1, "14/11/2021", 2, "sample_direct_eng_wc15112021_4.csv","W99999999", "W06000015", "W00010141", "W02000398", "C1", 833),
            (12, "F", "S7", "J4", "N", "Wales", "AB", 1, "14/11/2021", 4, "sample_direct_eng_wc15112021_4.csv", "W99999999", "W06000015", "W00010141", "W02000398", "C1", 833),
        ]
    )
    tranche_df = spark_session.createDataFrame(
        schema="""
            enrolement_date string,
            UAC integer,
            lsoa_11 string,
            cis20cd string,
            ctry12 string,
            ctry_name12 string,
            tranche integer
        """,
        data=[
            ("18/09/2021", 1, "S1", "J5", "E", "England", 2),
            ("18/10/2021", 2, "S2", "J5", "E", "England", 1),
            ("18/10/2021", 3, "S2", "J2", "E", "England", 2),
            ("18/10/2021", 4, "S4", "J2", "S", "Scotland", 2),
            ("18/10/2021", 5, "S3", "J4", "S", "Scotland", 2),
            ("18/10/2021", 6, "S3", "J3", "N", "NorthernIreland", 1),
            ("18/10/2021", 7, "S5", "J2", "N", "NorthernIreland", 1),
            ("18/10/2021", 8, "S1", "J3", "W", "Wales", 1),
            ("18/10/2021", 9, "S5", "J1", "E", "Wales", 2),
        ]
    )

    auxillary_dfs = {
        "old": old_sample_df,
        "new": new_sample_df,
        "nspl_lookup": nspl_lookup,
        "address_lookup": address_lookup,
        "country_lookup": country_lookup,
        "cis20cd_lookup": cis_20_lookup,
        "tranche": tranche_df,
    }

    # header = "cis_area_code_20,ons_household_id,postcode,lower_super_output_area_code_11,country_code_12,country_name_12,tranche_number_indicator,sample_source,sample_addressbase_indicator,date_sample_created,batch_number,file_name,household_level_designweight_swab,household_level_designweight_antibodies,region_code,local_authority_unity_authority_code,output_area_code_11/census_output_area_classification_11,middle_super_output_area_code_11,rural_urban_classification_11,index_multiple_deprivation,LSOA11NM,RGN19CD,sample_new_previous,enrolement_date,lsoa_11,cis20cd,ctry12,ctry_name12,tranche,tranche_eligible_households,number_eligible_households_tranche_bystrata_enrolment,number_sampled_households_tranche_bystrata_enrolment,tranche_factor,number_of_households_population_by_cis,number_of_households_population_by_country,number_eligible_household_sample,raw_design_weights_swab,sum_raw_design_weight_swab_cis,standard_deviation_raw_design_weight_swab,mean_raw_design_weight_swab,coefficient_variation_design_weight_swab,design_effect_weight_swab,effective_sample_size_design_weight_swab,sum_effective_sample_size_design_weight_swab,combining_factor_design_weight_swab,combined_design_weight_swab,sum_combined_design_weight_swab,scaling_factor_combined_design_weight_swab,scaled_design_weight_swab_nonadjusted,raw_design_weight_antibodies_ab,raw_design_weight_antibodies_c,sum_raw_design_weight_antibody_cis,standard_deviation_raw_design_weight_antibody,mean_raw_design_weight_antibody,coefficient_variation_design_weight_antibody,design_effect_weight_antibody,effective_sample_size_design_weight_antibody,sum_effective_sample_size_design_weight_antibody,combining_factor_design_weight_antibody,combined_design_weight_antibody,validated_design_weights,carryforward_design_weight_antibodies,sum_carryforward_design_weight_antibodies,scaling_factor_carryforward_design_weight_antibodies,scaled_design_weight_antibodies_nonadjusted"

    expected_df_path = "tests/weights/test_files/output1.csv"
    expected_df = spark_session.read.csv(
        expected_df_path, header=True, schema=None, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True, sep=","
    )

    output_df = generate_weights(auxillary_dfs=auxillary_dfs)
    # output_df.toPandas().to_csv("output1.csv", index=False)
    for col, type in output_df.dtypes:
        expected_df = expected_df.withColumn(col, F.col(col).cast(type))
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
