from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_uni_v2_rules


def test_flag_records_for_uni_v2_rules(spark_session):
    """Test flag_records_for_uni_v2_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        ("Employed and currently working", 1, 35, False),
        ("Employed and currently not working", 2, 99, False),
        ("Self-employed and currently working", 3, 12, False),
        ("Self-employed and currently not working", 4, 56, False),
        ("Looking for paid work and able to start", 5, 16, False),
        ("Looking for paid work and able to start", 5, 17, True),
        ("Not working and not looking for work", 6, 16, False),
        ("Not working and not looking for work", 6, 17, True),
        ("Retired", 7, 16, False),
        ("Retired", 7, 17, True),
        ("Child under 4-5y not attending child care", 8, 99, False),
        ("Child under 4-5y attending child care", 9, 12, False),
        ("4-5y and older at school/home-school", 10, 1, False),
        ("Attending college or FE (including if temporarily absent)", 11, 13, False),
        ("Attending university (including if temporarily absent)", 12, 17, False),
        (None, None, 18, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_status_v2 string, my_value int, age_at_visit int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_uni_v2_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
