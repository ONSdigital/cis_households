import pytest
from chispa import assert_df_equality

from cishouseholds.derive import derive_patient_facing_variables


# @pytest.mark.xfail(reason="debug patient_facing_variable() function")
def test_derive_patient_facing_variables(spark_session):
    not_working_str = "Not working (unemployed, retired, long-term sick etc.)"
    df_expected = spark_session.createDataFrame(
        data=[
            # fmt: off
            # testing:
            #id      work_status        patient_facing
            #                                       work_direct_contact_patients
            #                                                   job_title
            # health_care_classification == yes/no
            (1,     'Employed',         'Yes',       'Yes',     'NURSE',                'Yes',  'Yes',              "No",    'Working'), # Yes
            (2,     'Employed',         'Yes',       'Yes',     'DOCTOR',               'Yes',  'Yes',              "No",    'Working'), # Yes
            (3,     'Employed',         'No',        'Yes',     'TEACHER',              'No',   'Not Healthcare',   "No",    'Working'), # No
            (4,     'Student',          'No',        'Yes',     'TEACHER',              'No',   'Not Healthcare',   "No",    'Not working'), # No

            # patient_facing_classification == Yes / No / Not healthcare
            (5,     'Employed',         'Yes',      'Yes',      'CAREER',               'Yes',  'Yes',              "No",    'Working'), # Yes
            (6,     'Employed',         'Yes',      'No',       'TEACHER ASSISTANT',    'No',   'Not Healthcare',   "No",    'Working'), # No
            (7,     'Employed',         'No',       'Yes',      'HOSPITAL RECEPTIONIST','No',   'No',               "Yes",   'Working'),
            # work_status_classification
            (8,     'Employed',         'Yes',      'Yes',      'TEACHER ASSISTANT',    'No',   'Not Healthcare',   "No",    'Working'), # Yes
            (9,     not_working_str,    'Yes',      'Yes',      'TEACHER ASSISTANT',    'No',   'Not Healthcare',   "No",    'Not working'), # No
            (10,    not_working_str,    'Yes',      'Yes',      'TEACHER ASSISTANT',    'No',   'Not Healthcare',   "No",    'Not working'), # No

            # patient_facing_over_20_percent
            (11,    'Employed',         'Yes',      'Yes',      'NURSE',                'No',   'Yes',              "Yes",    'Working'), # testing pf +20%
            (11,    'Employed',         'Yes',      'Yes',      'NURSE',                'No',   'Yes',              "Yes",    'Working'),
            (11,    'Employed',         'No',       'Yes',      'NURSE',                'No',   'Not Healthcare',   "Yes",    'Working'),
            (11,    'Employed',         'No',       'Yes',      'NURSE',                'No',   'Not Healthcare',   "Yes",    'Working'),
            (11,    'Employed',         'No',       'Yes',      'TEACHER ASSISTANT',    'No',   'Not Healthcare',   "Yes",    'Working'),
            # fmt: on
        ],
        schema="""
         id integer,

         work_status string,
         patient_facing string,
         work_direct_contact_patients string,
         job_title string,

         health_care_classification string,
         patient_facing_classification string,
         participant_patient_facing_more_than_20_percent string,
         work_status_classification string
      """,
    )
    df_input = df_expected.drop(
        "health_care_classification",
        "patient_facing_classification",
        "participant_patient_facing_more_than_20_percent",
        "work_status_classification",
    )
    df_output = derive_patient_facing_variables(
        df=df_input,
        id_column_name="id",
        work_status_column_name="work_status",
        patient_facing_column_name="patient_facing",
        work_direct_contact_patients_column_name="work_direct_contact_patients",
        job_main_resp_column_name="job_title",
    )
    assert_df_equality(df_expected, df_output, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
