from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_event


def test_fill_forward_event(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-07","A","2020-01-01","some different detail"),
            (1,"2019-12-01","B","2020-01-01","some detail"),
            (1,"2020-01-05","C","2020-01-01","some detail"),
            (1,"2020-01-06","D",None,        "some detail"),
            (1,"2020-01-08","E","2020-01-07","some detail"),
            (1,"2020-02-01","F","2020-02-02","some detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2019-12-01","No",  None,        None),
            (1,"2020-01-05","Yes","2020-01-01","some detail"),
            (1,"2020-01-06","Yes","2020-01-01","some detail"),
            (1,"2020-01-07","Yes","2020-01-07","some detail"),
            (1,"2020-01-08","Yes","2020-01-07","some detail"),
            (1,"2020-02-01","Yes","2020-01-07","some detail")
        # fmt:on
    ]
    schema = """
            participant_id integer,
            visit_date string,
            event_indicator string,
            event_date string,
            detail string"""
    input_df = spark_session.createDataFrame(data=input_data, schema=schema)

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_event(
        df=input_df,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
