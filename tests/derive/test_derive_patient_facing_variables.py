import pytest
from chispa import assert_df_equality

from cishouseholds.derive import derive_patient_facing_variables


@pytest.mark.xfail(reason="debug patient_facing_variable() function")
def test_derive_patient_facing_variables(spark_session):
    df_expected = spark_session.createDataFrame(
        data=[
            # fmt: off
            # testing:
            # - patient_facing_over_20_percent

            #id      work_status   patient_facing
            #                                   work_direct_contact_patients
            #                                               job_title           job_role
            # health_care_classification
            (1,     'working',     'Yes',       'Yes',      'nurse',            'career',       'Yes',  'healthcare',       None,    'Yes'), # Yes
            (2,     'working',     'Yes',       'Yes',      'doctor',           'doctor',       'Yes',  'healthcare',       None,    'Yes'), # Yes
            (3,     'working',     'No',        'Yes',      'teacher',          'supervise',    'No',   'Not healthcare',   None,    'Yes'), # No

            # patient_facing_classification
            (3,     'working',      'Yes',      'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'Yes'), # Yes
            (4,     'working',      'Yes',      'No',       'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'Yes'), # No

            # work_status_classification
            (5,     'working',      'Yes',      'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'Yes'), # Yes
            (6,     'unemployed',   'Yes',      'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'), # No
            (7,     'Not working',  'Yes',      'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'), # No

            # patient_facing_over_20_percent
            (8,     'working',      'No',       'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'),
            (8,     'working',      'Yes',      'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'),
            (8,     'working',      'No',       'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'),
            (8,     'working',      'No',       'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'),
            (8,     'working',      'No',       'Yes',      'teacher assistant', 'education',    'No',  'Not healthcare',   None,    'No'),
            # fmt: on
        ],
        schema="""
         id integer,

         work_status string,
         patient_facing string,
         work_direct_contact_patients string,
         job_title string,
         job_role string,

         health_care_classification string,
         patient_facing_classification string,
         patient_facing_over_20_percent string,
         work_status_classification string
      """,
    )
    df_input = df_expected.drop(
        "health_care_classification",
        "patient_facing_classification",
        "patient_facing_over_20_percent",
        "work_status_classification",
    )
    df_output = derive_patient_facing_variables(
        df=df_input,
        id_column_name="id",
        work_status_column_name="work_status",
        patient_facing_column_name="patient_facing",
        work_direct_contact_patients_column_name="work_direct_contact_patients",
        job_title_column_name="job_title",
        job_role_column_name="job_role",
    )
    # import pdb; pdb.set_trace()
    assert_df_equality(df_expected, df_output, ignore_column_order=True, ignore_row_order=True)
