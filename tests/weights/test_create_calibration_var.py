from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import create_calibration_var


def test_create_calibration_var(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                ('england',			    1,1,1,16, 3, 2,1,	1,		1, 1, 1,1, 1,1,		    3, 		None, 	2,		2, 		None,	    2,		1, 		1, 1, 1, 1, 1, 1),
                ('wales',				1,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None,		None, 	None, 	1, None, 1, 1, None, None),
                ('northern_ireland',	2,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None, 		None, 	None, 	1, None, 1, 1, None, None),
                ('scotland',			3,1,1,16, 3, 2,1, 	None,	1, 1, 1,1, 1,1, 		None,	3,		None, 	None,	None, 		None, 	None, 	1, None, 1, 1, None, None),
            # fmt: on
        ],
        schema="""
                country_name_12 string,
                interim_region_code integer,
                interim_sex integer,
                ethnicity_white integer,
                age_at_visit integer,
                age_group_swab integer,
                age_group_antibodies integer,

                swab integer,
                antibodies integer,
                ever_never integer,
                longcovid integer,
                14_days integer,
                28_days integer,
                24_days integer,
                42_days integer,

                p1_swab_longcovid_england integer,
                p1_swab_longcovid_wales_scot_ni integer,
                p1_for_antibodies_evernever_engl integer,
                p1_for_antibodies_28daysto_engl integer,
                p1_for_antibodies_wales_scot_ni integer,
                p2_for_antibodies integer,
                p3_for_antibodies_28daysto_engl integer,

                swab_evernever integer,
                swab_14days integer,
                longcovid_24days integer,
                longcovid_42days integer,
                antibodies_evernever integer,
                antibodies_28daysto integer
            """,
    )

    input_df = expected_df.drop(
        "p1_swab_longcovid_england",
        "p1_swab_longcovid_wales_scot_ni",
        "p1_for_antibodies_evernever_engl",
        "p1_for_antibodies_28daysto_engl",
        "p1_for_antibodies_wales_scot_ni",
        "p2_for_antibodies",
        "p3_for_antibodies_28daysto_engl",
        "swab_evernever",
        "swab_14days",
        "longcovid_24days",
        "longcovid_42days",
        "antibodies_evernever",
        "antibodies_28daysto",
    )
    output_df = create_calibration_var(df=input_df)
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
