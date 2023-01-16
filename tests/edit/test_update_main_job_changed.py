import pytest
from chispa import assert_df_equality

from cishouseholds.edit import update_work_main_job_changed

# fmt: off

@pytest.mark.parametrize(
    ("input_data","expected_data"),
    (
        # change based only on title and role
        (
            [
                (1, None,      None,       None,   None,   None,   None),
                (1, "job_1",   "role_1",   None,   None,   None,   None),      # set work_main_job_changed to "Yes"
                (1, "job_1",   "role_1",   None,   None,   None,   None),      # do nothing
                (1, None,      None,       None,   None,   None,   None),      # do nothing
                (1, "job_2",   "role_2",   None,   None,   None,   None),      # set work_main_job_changed to "Yes"
            ]
        ,
            [
                (1, None,      None,       None,   None,   None,   None,   "No"),      # do nothing
                (1, "job_1",   "role_1",   None,   None,   None,   None,   "Yes"),     # set work_main_job_changed to "Yes"
                (1, "job_1",   "role_1",   None,   None,   None,   None,   "No"),      # do nothing
                (1, None,      None,       None,   None,   None,   None,   "No"),      # do nothing
                (1, "job_2",   "role_2",   None,   None,   None,   None,   "Yes"),     # set work_main_job_changed to "Yes"
            ]
        ),
        # change based on role and sector
        (
            [
                (1, "job_1",   "role_1",   "sc",   None,   None,   None),      # set work_main_job_changed to "Yes"
                (1, "job_1",   "role_1",   "sc",   None,   None,   None),      # do nothing
                (1, None,      None,       None,   None,   None,   None),      # do nothing
                (1, "job_1",   "role_2",   "hc",   None,   None,   None),      # set work_main_job_changed to "Yes"
            ]
        ,
            [
                (1, "job_1",   "role_1",   "sc",   None,   None,   None,   "Yes"),     # set work_main_job_changed to "Yes"
                (1, "job_1",   "role_1",   "sc",   None,   None,   None,   "No"),      # do nothing
                (1, None,      None,       None,   None,   None,   None,   "No"),      # do nothing
                (1, "job_1",   "role_2",   "hc",   None,   None,   None,   "Yes"),     # set work_main_job_changed to "Yes"
            ]
        ),
        # change based on patients
        (
            [
                (1, "job_1",   None,       "hc",   None,   None,   "No"),      # set work_main_job_changed to "Yes"
                (1, None,      None,       None,   None,   None,   None),      # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "No"),      # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "Yes"),      # set work_main_job_changed to "Yes"
            ]
        ,
            [
                (1, "job_1",   None,       "hc",   None,   None,   "No",   "Yes"),     # set work_main_job_changed to "Yes"
                (1, None,      None,       None,   None,   None,   None,   "No"),      # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "No",   "Yes"),     # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "Yes",  "Yes"),     # set work_main_job_changed to "Yes"
            ]
        ),
        # rows 1 and 3 change based on patients
        (
            [
                (1, "job_1",   None,       "hc",   None,   None,   "No"),      # set work_main_job_changed to "Yes"
                (1, "job_1",   None,       "hc",   None,   None,   "No"),      # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "Yes"),      # set work_main_job_changed to "Yes"
                (1, None,      None,       None,   None,   None,   None),      # do nothing
            ]
        ,
            [
                (1, "job_1",   None,       "hc",   None,   None,   "No",   "Yes"),     # set work_main_job_changed to "Yes"
                (1, "job_1",   None,       "hc",   None,   None,   "No",   "No"),      # do nothing
                (1, "job_1",   None,       "hc",   None,   None,   "Yes",  "Yes"),     # set work_main_job_changed to "Yes"
                (1, None,      None,       None,   None,   None,   None,   "No"),      # do nothing
            ]
        ),
    )
)
def test_update_work_main_job_changed(spark_session, input_data, expected_data):
    input_df = spark_session.createDataFrame(
        data=input_data,
        schema="id string, title string, role string, sector string, hc_area integer, sc_area string, patients string",
    )
    expected_df = spark_session.createDataFrame(
        data=expected_data,
        schema="id string, title string, role string, sector string, hc_area integer, sc_area string, patients string, work_main_job_changed string",
    )
    output_df = update_work_main_job_changed(
        input_df,
        column_name_to_update="work_main_job_changed",
        participant_id_column="id",
        reference_not_null_columns=[
            "title",
            "role",
            "sector",
        ],
        reference_value_columns=[
            "patients"
        ],
        value="Yes",
    )
    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=False, ignore_column_order=False)
