import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.impute import post_imputation_wrapper


def test_post_imputation_wrapper(spark_session):
    survey_df_input = spark_session.createDataFrame(
        data=[
            # fmt:off
            ('id1',     '2020-01-01',  'hid1',     'White',    'Male',  '1999-01-01'), # testing that id1 gets filled in on one imputation column
            ('id1',     '2020-01-02',  'hid1',     None,       'Male',  '1999-01-01'),
            ('id1',     '2020-01-03',  'hid1',     None,       'Male',  '1999-01-01'),

            ('id2',     '2020-01-01',  'hid1',     'White',   'Male',  '1999-01-01'), # testing that id1 gets filled in on multiple imputation column
            ('id2',     '2020-01-02',  'hid1',     'unknown',  None,    None),
            ('id2',     '2020-01-03',  'hid1',     None,       None,    None),

            ('id5',     '2020-01-03',  'hid1',     None,       None,    None),
            # test that id5 is not filtered out
            # fmt:on
        ],
        schema="""
            participant_id string,
            visit_datetime string,
            ons_household_id string,
            ethnicity_white string,
            sex string,
            date_of_birth string
        """,
    )
    key_columns_imputed_df_input = spark_session.createDataFrame(
        data=[
            # fmt:off
            ('id1',     'White',    "type_imputation_1e", 1,      'Male',     "type_imputation_1s", 1,   '1999-01-01',     "type_imputation_1d", 1),
            ('id2',     'purple',   "type_imputation_2e", 1,      'Male',     "type_imputation_2s", 1,   '1999-01-01',     "type_imputation_2d" ,1), # testing that id1 gets filled in on multiple imputation column

            # testing that step 1 in post_imputation_wrapper is included correctly for id3 for 2 non inputed cases
            ('id3',     'green',    None, None,                      'female',   "type_imputation_3s", 1,   '1989-01-01',     None, None),

            # testing that step 1 in post_imputation_wrapper gets filtered out for id4 for all non inputed cases
            ('id4',     'green',    None,   None,                     'female',   None, None,                  '1989-01-01',     None, None),
            # fmt:on
        ],
        # not_wanted_col to make sure its filtered out in step 2
        schema="""
            participant_id string,
            ethnicity_white string,
            ethnicity_white_imputation_method string,
            ethnicity_white_is_imputed integer,
            sex string,
            sex_imputation_method string,
            sex_is_imputed integer,
            date_of_birth string,
            date_of_birth_imputation_method string,
            date_of_birth_is_imputed integer
        """,
    )
    df_imputed_values_expected = spark_session.createDataFrame(
        data=[
            # fmt:off
            ('id1',	'2020-01-01',	'hid1',		'White',		'type_imputation_1e', 1,	'Male',   	'type_imputation_1s', 1,   '1999-01-01',        	'type_imputation_1d', 1),
            ('id1',	'2020-01-02',	'hid1',		'White',		'type_imputation_1e', 1,	'Male',   	'type_imputation_1s', 1,   '1999-01-01',        	'type_imputation_1d', 1),
            ('id1',	'2020-01-03',	'hid1',		'White',		'type_imputation_1e', 1,	'Male',   	'type_imputation_1s', 1,   '1999-01-01',        	'type_imputation_1d', 1),
            ('id2',	'2020-01-01',	'hid1',		'purple',	'type_imputation_2e', 1,	'Male',   	'type_imputation_2s', 1,   '1999-01-01',        	'type_imputation_2d', 1),
            ('id2',	'2020-01-02',	'hid1',		'purple',	'type_imputation_2e', 1,	'Male',   	'type_imputation_2s', 1,   '1999-01-01',        	'type_imputation_2d', 1),
            ('id2',	'2020-01-03',	'hid1',		'purple',	'type_imputation_2e', 1,	'Male',   	'type_imputation_2s', 1,   '1999-01-01',        	'type_imputation_2d', 1),
            ('id5',	'2020-01-03',	'hid1',		None,			None, None,                   None,       None, None,                   None,                   None, None),
            # fmt:on
        ],
        schema="""
            participant_id string,
            visit_datetime string,
            ons_household_id string,
            ethnicity_white string,
            ethnicity_white_imputation_method string,
            ethnicity_white_is_imputed integer,
            sex string,
            sex_imputation_method string,
            sex_is_imputed integer,
            date_of_birth string,
            date_of_birth_imputation_method string,
            date_of_birth_is_imputed integer
        """,
    )

    expected_lookup_df = spark_session.createDataFrame(
        data=[
            # fmt:off
            ('id1',     'White',    "type_imputation_1e",     'Male',     "type_imputation_1s",  '1999-01-01',     "type_imputation_1d"),
            ('id2',     'purple',   "type_imputation_2e",     'Male',     "type_imputation_2s",  '1999-01-01',     "type_imputation_2d"), # testing that id1 gets filled in on multiple imputation column
            ('id3',     'green',    None,                      'female',   "type_imputation_3s",   '1989-01-01',     None),
            # fmt:on
        ],
        # not_wanted_col to make sure its filtered out in step 2
        schema="""
            participant_id string,
            ethnicity_white string,
            ethnicity_white_imputation_method string,
            sex string,
            sex_imputation_method string,
            date_of_birth string,
            date_of_birth_imputation_method string
        """,
    )

    df_imputed_values_output, imputed_lookup_output = post_imputation_wrapper(
        df=survey_df_input, key_columns_imputed_df=key_columns_imputed_df_input
    )
    assert_df_equality(
        df_imputed_values_expected, df_imputed_values_output, ignore_row_order=True, ignore_column_order=True
    )
    assert_df_equality(expected_lookup_df, imputed_lookup_output, ignore_row_order=True, ignore_column_order=True)
