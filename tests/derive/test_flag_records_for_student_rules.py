from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_school_v2_rules
from cishouseholds.derive import flag_records_for_student_v0_rules
from cishouseholds.derive import flag_records_for_student_v1_rules


def test_flag_records_for_student_v0_rules(spark_session):
    """Test flag_records_for_student_v0_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        ("Employed", 17, True),
        ("Self-employed", 19, False),
        ("Furloughed (temporarily not working)", 21, True),
        ("Not working (unemployed, retired, long-term sick etc.)", 17, True),
        ("Student", 19, False),
        (None, 23, True),
        (None, 15, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_status_v0 string, age_at_visit int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_student_v0_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_student_v1_rules(spark_session):
    """Test flag_records_for_student_v1_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v1']
    test_cases = [
        ("Employed and currently working", 1, 25, False),
        ("Employed and currently not working", 2, 3, False),
        ("Self-employed and currently working", 3, 35, False),
        ("Self-employed and currently not working", 4, 19, False),
        ("Looking for paid work and able to start", 5, 18, True),
        ("Looking for paid work and able to start", 5, 5, True),
        ("Not working and not looking for work", 6, 23, True),
        ("Not working and not looking for work", 6, 6, True),
        ("Retired", 7, 99, True),
        ("Retired", 7, 9, True),
        ("Child under 5y not attending child care", 8, 55, True),
        ("Child under 5y not attending child care", 8, 6, True),
        ("Child under 5y attending child care", 9, 55, True),
        ("Child under 5y attending child care", 9, 17, True),
        ("5y and older in full-time education", 10, 67, False),
        (None, None, 18, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_status_v1 string, my_value int, age_at_visit int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_student_v1_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_school_v2_rules(spark_session):
    """Test flag_records_for_school_v2_rules function correctly flags the records"""

    test_cases = [
        (1, None, False),
        (5, 8, True),
        (12, None, False),
        (19, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="age_at_visit int, school_year int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_school_v2_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
