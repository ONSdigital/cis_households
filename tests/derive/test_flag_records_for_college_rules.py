from chispa import assert_df_equality

from cishouseholds.regex.regex_flags import flag_records_for_college_v0_rules
from cishouseholds.regex.regex_flags import flag_records_for_college_v2_rules


def test_flag_records_for_college_v2_rules(spark_session):
    """Test flag_records_for_college_v2_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v2']
    test_cases = [
        ("Employed and currently working", 35, False),
        ("Employed and currently not working", 99, True),
        ("Self-employed and currently working", 12, False),
        ("Self-employed and currently not working", 56, True),
        ("Looking for paid work and able to start", 15, False),
        ("Looking for paid work and able to start", 16, True),
        ("Not working and not looking for work", 15, False),
        ("Not working and not looking for work", 16, True),
        ("Retired", 60, True),
        ("Child under 4-5y not attending child care", 99, False),
        ("Child under 4-5y attending child care", 12, False),
        ("4-5y and older at school/home-school", 1, False),
        ("Attending college or FE (including if temporarily absent)", 13, False),
        ("Attending university (including if temporarily absent)", 17, True),
        (None, 16, True),
        (None, 15, False),
        (None, 17, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_status_v2 string, age_at_visit int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_college_v2_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_flag_records_for_college_v0_rules(spark_session):
    """Test flag_records_for_college_v0_rules function correctly flags the records"""

    # the following is from cishouseholds.mapping.category_maps['iqvia_raw_category_map']['work_status_v0']
    test_cases = [
        ("Employed", 35, False),
        ("Self-employed", 99, False),
        ("Furloughed (temporarily not working)", 12, False),
        ("Furloughed (temporarily not working)", 17, True),
        ("Not working (unemployed, retired, long-term sick etc.)", 15, False),
        ("Not working (unemployed, retired, long-term sick etc.)", 21, True),
        (None, 15, False),
        (None, 16, True),
        (None, 17, True),
        (None, 25, True),
    ]

    expected_df = spark_session.createDataFrame(
        test_cases, schema="work_status_v0 string, age_at_visit int, actual_flag boolean"
    )

    actual_df = expected_df.drop("actual_flag").withColumn("actual_flag", flag_records_for_college_v0_rules())

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
        ignore_nullable=True,
    )
