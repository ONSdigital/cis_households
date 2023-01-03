from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_not_working_rules_v0
from cishouseholds.derive import flag_records_for_not_working_rules_v1_a
from cishouseholds.derive import flag_records_for_not_working_rules_v1_b
from cishouseholds.derive import flag_records_for_not_working_rules_v2_a
from cishouseholds.derive import flag_records_for_not_working_rules_v2_b


def test_flag_records_for_not_working_rules_v0(spark_session):
    """Test flag_records_for_not_working_rules_v0 function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        (0, "Employed", 1, True),
        (1, "Self-employed", 2, False),
        (0, "Furloughed (temporarily not working)", 3, False),
        (0, "Not working (unemployed, retired, long-term sick etc.)", 4, False),
        (0, "Student", 5, False),
        (1, None, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v0 string, my_value int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_not_working_rules_v0())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_not_working_rules_v1_a(spark_session):
    """Test flag_records_for_not_working_rules_v1_a function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v1']
    test_cases = [
        (1, "Employed and currently working", 1, True),
        (2, "Employed and currently working", 1, False),
        (0, "Employed and currently not working", 2, False),
        (1, "Self-employed and currently working", 3, False),
        (1, "Self-employed and currently not working", 4, False),
        (1, "Looking for paid work and able to start", 5, False),
        (1, "Not working and not looking for work", 6, False),
        (1, "Retired", 7, False),
        (1, "Child under 5y not attending child care", 8, False),
        (1, "Child under 5y attending child care", 9, False),
        (1, "5y and older in full-time education", 10, False),
        (1, None, None, None),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v1 string, my_value int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_not_working_rules_v1_a())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_not_working_rules_v1_b(spark_session):
    """Test flag_records_for_not_working_rules_v1_b function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v1']
    test_cases = [
        (1, "Employed and currently working", 1, False),
        (1, "Employed and currently not working", 2, False),
        (1, "Self-employed and currently working", 3, True),
        (2, "Self-employed and currently working", 3, False),
        (1, "Self-employed and currently not working", 4, False),
        (1, "Looking for paid work and able to start", 5, False),
        (1, "Not working and not looking for work", 6, False),
        (1, "Retired", 7, False),
        (0, "Child under 5y not attending child care", 8, False),
        (2, "Child under 5y attending child care", 9, False),
        (1, "5y and older in full-time education", 10, False),
        (1, None, None, None),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v1 string, my_value int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_not_working_rules_v1_b())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_not_working_rules_v2_a(spark_session):
    """Test flag_records_for_not_working_rules_v2_a function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        (2, "Employed and currently working", 1, True),
        (0, "Employed and currently working", 1, False),
        (2, "Employed and currently not working", 2, False),
        (2, "Self-employed and currently working", 3, False),
        (2, "Self-employed and currently not working", 4, False),
        (1, "Looking for paid work and able to start", 5, False),
        (0, "Not working and not looking for work", 6, False),
        (2, "Retired", 7, False),
        (1, "Child under 4-5y not attending child care", 8, False),
        (2, "Child under 4-5y attending child care", 9, False),
        (2, "4-5y and older at school/home-school", 10, False),
        (2, "Attending college or FE (including if temporarily absent)", 11, False),
        (2, "Attending university (including if temporarily absent)", 12, False),
        (2, None, None, None),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v2 string, my_value int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_not_working_rules_v2_a())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_not_working_rules_v2_b(spark_session):
    """Test flag_records_for_not_working_rules_v2_b function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        (2, "Employed and currently working", 1, False),
        (2, "Employed and currently not working", 2, False),
        (2, "Self-employed and currently working", 3, True),
        (0, "Self-employed and currently working", 3, False),
        (2, "Self-employed and currently not working", 4, False),
        (2, "Looking for paid work and able to start", 5, False),
        (1, "Not working and not looking for work", 6, False),
        (2, "Retired", 7, False),
        (2, "Child under 4-5y not attending child care", 8, False),
        (2, "Child under 4-5y attending child care", 9, False),
        (0, "4-5y and older at school/home-school", 10, False),
        (2, "Attending college or FE (including if temporarily absent)", 11, False),
        (2, "Attending university (including if temporarily absent)", 12, False),
        (2, None, None, None),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v2 string, my_value int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_not_working_rules_v2_b())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
