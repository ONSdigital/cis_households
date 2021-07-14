from chispa import assert_df_equality

from cishouseholds.derive import assign_from_lookup


def test_assign_from_lookup(spark_session):
    column_name_to_assign = "contact_hospital"
    column_names = ["contact_participant_hospital", "contact_other_in_hh_hospital"]
    expected_df = spark_session.createDataFrame(
        data=[
            ("No", "No", 0),
            ("Yes", "No", 1),
            ("Yes", "Yes", 1),
            ("Yes", None, 1),
            ("Yes", "Participant would not...", 1),
            ("No", "Yes", 2),
            ("No", None, None),
            ("No", "Participant would not...", None),
        ],
        schema=column_names + [column_name_to_assign],
    )

    input_df = expected_df.drop(column_name_to_assign)

    lookup_df = spark_session.createDataFrame(
        data=[
            ("No", "No", 0),
            ("Yes", "No", 1),
            ("Yes", "Yes", 1),
            ("Yes", None, 1),
            ("Yes", "Participant would not...", 1),
            ("No", "Yes", 2),
        ],
        schema=column_names + [column_name_to_assign],
    )

    actual_df = assign_from_lookup(input_df, column_name_to_assign, column_names, lookup_df)

    assert_df_equality(actual_df, expected_df)


if __name__ == "__main__":
    pass
