from chispa import assert_df_equality

from cishouseholds.derive import derive_patient_facing_variables


def test_derive_patient_facing_variables(spark_session):
    df_expected = spark_session.createDataFrame(
        data=[
            # fmt: off
         (1,      'working',     'yes',      'yes',      'teacher assistant', 'education',      True,    True,    True,    True),
            # fmt: on
        ],
        schema="""
         id integer,

         work_status string,
         patient_facing string,
         work_direct_contact_patients string,
         job_title string,
         job_role string,

         health_care_classification boolean,
         patient_facing_classification boolean,
         patient_facing_over_20_percent boolean,
         work_status_classification boolean
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
        work_status_column_name="work_status",
        patient_facing_column_name="patient_facing",
        work_direct_contact_patients_column_name="work_direct_contact_patients",
        job_title_column_name="job_title",
        job_role_column_name="job_role",
    )
    import pdb

    pdb.set_trace()

    assert_df_equality(df_expected, df_output, ignore_column_order=True, ignore_row_order=True)
