from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_childcare_v0_rules
from cishouseholds.derive import flag_records_for_childcare_v1_rules
from cishouseholds.derive import flag_records_for_childcare_v2_b_rules

# FMT:OFF


def test_flag_records_for_childcare_v0_rules(spark_session):
    """Test flag_records_for_childcare_v0_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        (0, "Not working (unemployed, retired, long-term sick etc.)", 35, None, False),
        (0, "Employed", 2, None, True),
        (1, "Not working (unemployed, retired, long-term sick etc.)", 2, None, False),
        (0, "Employed", 3, None, True),
        (0, "Furloughed (temporarily not working)", 3, 1, False),
        (0, None, 16, 11, False),
        (1, None, 15, 10, False),
        (2, None, 16, 11, False),
        (0, None, 17, 12, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v0 string, age_at_visit int, school_year int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_childcare_v0_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_childcare_v1_rules(spark_session):
    """Test flag_records_for_childcare_v1_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v1']
    test_cases = [
        (1, "Employed", 4, None, True),
        (1, "Self-employed", 1, None, True),
        (0, "Self-employed", 1, None, False),
        (1, "Self-employed", 4, 1, False),
        (1, "Looking for paid work and able to start", 17, None, False),
        (1, "Not working and not looking for work", 16, None, False),
        (1, "Child under 5y not attending child care", 4, None, False),
        (1, "Retired", 4, None, True),
        (1, None, 15, 10, False),
        (1, None, 16, 11, False),
        (0, None, 17, 12, False),
        (2, None, 25, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v1 string, age_at_visit int, school_year int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_childcare_v1_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_childcare_v2_b_rules(spark_session):
    """Test flag_records_for_childcare_v2_b_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        (0, "Employed", 5, 1, False),
        (2, "Employed", 5, None, True),
        (2, "Not working and not looking for work", 12, 7, False),
        (2, "Not working and not looking for work", 2, None, True),
        (2, "4-5y and older at school/home-school)", 5, 1, False),
        (0, "Not working (unemployed, retired, long-term sick etc.)", 21, None, False),
        (2, None, 15, 10, False),
        (2, None, 16, 11, False),
        (2, None, 17, 12, False),
        (1, None, 25, None, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v2 string, age_at_visit int, school_year int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_childcare_v2_b_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
