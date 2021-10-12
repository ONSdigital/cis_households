from chispa import assert_df_equality

from cishouseholds.edit import update_work_patient_facing_now


def test_update_work_patient_facing_now(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("Yes", "(temporarily not working)", 12),
            ("Yes", "something else", 25),
            ("Yes", "something else", 1),
            ("<=15y", "Furloughed", 69),
        ],
        schema="facing string, status string, age integer",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            ("No", "(temporarily not working)", 12),
            (25, "something else", 25),
            ("Yes", "something else", 1),
            ("No", "Furloughed", 69),
        ],
        schema="facing string, status string, age integer",
    )

    output_df = update_work_patient_facing_now(input_df, "age", "status", "facing")
    assert_df_equality(output_df, expected_df)
