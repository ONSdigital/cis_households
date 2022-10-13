from chispa import assert_df_equality

from cishouseholds.pipeline.reporting import count_variable_option

# import pyspark.sql import SparkSession


def test_count_variable_option(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-07","A","2020-01-01","uncodeable"),
            (1,"2019-12-01","B","2020-01-01","uncode"),
            (1,"2020-01-05","A","2020-01-01","uncodeable"),
            (1,"2020-01-06","C",None,"1"),
            (1,"2020-01-08","A","2020-01-07","Uncodeable"),
            (1,"2020-02-01","D","2020-02-02","uncodeabl"),
            (1,"2020-02-01","E","2020-02-02","uncodeable"),
            (1,"2020-02-01","E","2020-02-02","uncodeables"),
            (1,"2020-02-01","F","2020-02-02","uncodeables")
        # fmt:on
    ]

    expected_data = [("detail", "uncodeable", 3)]
    input_schema = """
            participant_id integer,
            visit_date string,
            event_indicator string,
            event_date string,
            detail string"""

    expected_schema = """
            column_name string,
            column_value string,
            count integer"""

    input_df = spark_session.createDataFrame(data=input_data, schema=input_schema)
    # import pdb; pdb.set_trace()
    actual_df = count_variable_option(input_df, "detail", "uncodeable")
    expected_df = spark_session.createDataFrame(data=expected_data, schema=expected_schema)
    # import pdb; pdb.set_trace()

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
