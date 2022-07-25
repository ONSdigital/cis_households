import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.pipeline.high_level_transformations import impute_demographic_columns_integration


def test_impute_demographic_columns_integration(spark_session):
    # TODO:
    # open lookup file and survey file
    # lookup columns:
    # survey columns:

    # INPUT ------------------------------------------------------------------------------------------------------------------------------------------
    survey_df_input = spark_session.createDataFrame(
        data=[
            # fmt:off

            ('id1',     '2020-01-01',  'hid1',     'White',    None,                'Male',  None,                '1999-01-01',  None),
            ('id1',     '2020-01-02',  'hid1',     None,       'k_nearest_mean',    None,    'k_nearest_mean',    None,          'k_nearest_mean'), # TESTING that imputation happens if user_id is not in lookup_df

            # testing that user_id=id2 is searched first into lookup_df table
            ('id2',     '2020-01-01',  'hid1',     'White',    None,                'Male',  None,                '1999-01-01',  None),
            ('id2',     '2020-01-02',  'hid1',     'White',    'k_nearest_mean',    'Male',  'k_nearest_mean',    '1999-01-01',  'k_nearest_mean'), # TESTING should be ovewritten by the lookup

            # testing that user_id=id3 is searched first into lookup_df table
            ('id3',     '2020-01-01',  'hid1',     'White',    None,                'Male',  None,                '1999-01-01',  None),
            ('id3',     '2020-01-02',  'hid1',     'White',    'k_nearest_mean',    'Male',  'k_nearest_mean',    '1999-01-01',  'k_nearest_mean'), # TESTING should be
            ('id3',     '2020-01-03',  'hid1',     'White',    'k_nearest_mean',    'Male',  'k_nearest_mean',    '1999-01-01',  'k_nearest_mean'),
            # TESTING should be
            # fmt:on
        ],
        schema="""
            participant_id string,
            visit_datetime string,
            ons_household_id string,

            ethnicity_white string,
            ethnicity_white_imputation_method string,
            sex string,
            sex_imputation_method string,
            date_of_birth string,
            date_of_birth_imputation_method string
        """,
    )
    for column_needed in ["cis_area_code_20", "region_code", "people_in_household_count_group", "work_status_group"]:
        survey_df_input = survey_df_input.withColumn(column_needed, None)

    imputed_lookup_df_input = spark_session.createDataFrame(
        data=[
            # fmt:off
            ('id2',        'Green', 'k_nearest_mean',       'Female',   'k_nearest_mean',       '1980-01-01',  'k_nearest_mean'),
            # fmt:on
        ],
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

    survey_df_output, imputed_lookup_df_output = impute_demographic_columns_integration(
        df=survey_df_input,
        imputed_value_lookup_df=imputed_lookup_df_input,
        log_directory="",
    )
    import pdb

    pdb.set_trace()
    # TODO:
    # compare survey_df to the expected_survey_df
    # compare lookup_df to the expected_lookup_df

    # EXPECTED ------------------------------------------------------------------------------------------------------------------------------------------
    survey_df_expected = spark_session.createDataFrame(
        data=[
            # fmt:off
            # user_id,     ethnicity string,    ethnicity_imputation_method,     sex,     sex_imputation_method,     dob,           dob_imputation_method
            ('id1',        'White',             None,                            'Male',  None,                      '1999-01-01',  None),
            ('id1',        'White',             'k_nearest_mean',                'Male',  'k_nearest_mean',          '1999-01-01',  'k_nearest_mean'), # TESTING that imputation happens if user_id is not in lookup_df

            # testing that user_id=id2 is searched first into lookup_df table
            ('id2',        'Green',             None,                            'Female',None,                      '1980-01-01',  None),
            ('id2',        'Green',             'k_nearest_mean',                'Female','k_nearest_mean',          '1980-01-01',  'k_nearest_mean'),
            # TESTING these should be ovewritten by the lookup
            # fmt:on
        ],
        schema="""
            user_id string,

            ethnicity string,
            ethnicity_imputation_method string,
            sex string,
            sex_imputation_method string,
            dob string,
            dob_imputation_method string
        """,
    )
    imputed_lookup_df_expected = spark_session.createDataFrame(
        data=[
            # fmt:off
            # user_id,     ethnicity string,    ethnicity_imputation_method,     sex,     sex_imputation_method,     dob,           dob_imputation_method
            ('id2',        'Green',             'k_nearest_mean',                'Female','k_nearest_mean',          '1980-01-01',  'k_nearest_mean'),
            # fmt:on
        ],
        schema="""
            user_id string,

            ethnicity string,
            ethnicity_imputation_method string,
            sex string,
            sex_imputation_method string,
            dob string,
            dob_imputation_method string
        """,
    )

    assert_df_equality(survey_df_expected, survey_df_output, ignore_row_order=True, ignore_column_order=True)
    assert_df_equality(
        imputed_lookup_df_expected, imputed_lookup_df_output, ignore_row_order=True, ignore_column_order=True
    )
