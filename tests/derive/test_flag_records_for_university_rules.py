from chispa import assert_df_equality

from cishouseholds.derive import flag_records_for_uni_v0_rules
from cishouseholds.derive import flag_records_for_uni_v1_rules
from cishouseholds.derive import flag_records_for_uni_v2_rules


def test_flag_records_for_uni_v2_rules(spark_session):
    """Test flag_records_for_uni_v2_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        (2, "Employed and currently working", 35, False),
        (2, "Employed and currently not working", 99, True),
        (2, "Self-employed and currently working", 12, False),
        (2, "Self-employed and currently not working", 56, True),
        (2, "Looking for paid work and able to start", 16, False),
        (2, "Looking for paid work and able to start", 17, True),
        (2, "Not working and not looking for work", 16, False),
        (2, "Not working and not looking for work", 17, True),
        (2, "Retired", 16, False),
        (2, "Retired", 17, True),
        (2, "Child under 4-5y not attending child care", 99, False),
        (2, "Child under 4-5y attending child care", 12, False),
        (2, "4-5y and older at school/home-school", 1, False),
        (2, "Attending college or FE (including if temporarily absent)", 13, False),
        (2, "Attending university (including if temporarily absent)", 19, True),
        (2, None, 17, True),
        (2, "Employed and currently working", 21, True),
        (2, None, 16, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v2 string, age_at_visit int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_uni_v2_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_uni_v1_rules(spark_session):
    """Test flag_records_for_uni_v1_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        (1, "Employed and currently working", 19, True),
        (2, "Self-employed and currently not working", 99, False),
        (1, "Not working and not looking for work", 12, False),
        (1, "Not working and not looking for work", 17, True),
        (0, "Not working and not looking for work", 17, False),
        (1, "Retired", 16, False),
        (1, "Not working and not looking for work", 21, True),
        (1, None, 17, True),
        (1, None, 16, False),
        (2, None, 25, False),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v1 string, age_at_visit int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_uni_v1_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_uni_v0_rules(spark_session):
    """Test flag_records_for_uni_v0_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        (0, "Employed", 35, False),
        (2, "Self-employed", 99, False),
        (0, "Furloughed (temporarily not working)", 12, False),
        (0, "Furloughed (temporarily not working)", 17, True),
        (2, "Furloughed (temporarily not working)", 17, False),
        (1, "Furloughed (temporarily not working)", 17, False),
        (0, "Not working (unemployed, retired, long-term sick etc.)", 16, False),
        (0, "Not working (unemployed, retired, long-term sick etc.)", 21, True),
        (0, None, 17, True),
        (0, None, 16, False),
        (0, None, 25, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases,
        schema="survey_response_dataset_major_version int, work_status_v0 string, age_at_visit int, actual_flag boolean",
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_uni_v0_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
