from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_event


def test_fill_forward_event(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-07","A","2020-01-01","some different detail"),
            (1,"2019-12-01","B","2020-01-01","some detail"),
            (1,"2020-01-05","C","2020-01-01","some detail"),
            (1,"2020-01-06","D",None,        "some detail"),
            (1,"2020-02-08","E","2020-02-07","some detail"),
            (1,"2020-03-01","F","2020-03-02","some detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2019-12-01","No",  None,        None),
            (1,"2020-01-05","Yes","2020-01-01","some detail"),
            (1,"2020-01-06","Yes","2020-01-01","some detail"),
            (1,"2020-01-07","Yes","2020-01-07","some detail"),
            (1,"2020-02-08","Yes","2020-02-07","some detail"),
            (1,"2020-03-01","Yes","2020-02-07","some detail")
        # fmt:on
    ]
    input_data_specific_case_1 = [
        # fmt:off
            (1,"2020-01-01","No",None,"some detail"),
            (1,"2019-12-02",None,None,"some detail"),
            (1,"2020-01-03","No",None,"some detail"),
            (1,"2020-01-04","No",None,"some detail"),
            (1,"2020-01-05",None,None,None),
        # fmt:on
    ]
    expected_data_specific_case_1 = [
        # fmt:off
            (1,"2020-01-01","No",None,"some detail"),
            (1,"2019-12-02","No",None,"some detail"),
            (1,"2020-01-03","No",None,"some detail"),
            (1,"2020-01-04","No",None,"some detail"),
            (1,"2020-01-05","No",None,None),
        # fmt:on
    ]
    input_data_specific_case_2 = [
        # fmt:off
            (1,"2020-06-01","No", "2020-01-01","detail1"),
            (1,"2020-06-02","Yes","2020-01-02","detail2"),
            (1,"2020-06-03","Yes","2020-01-03","detail3"),
            (1,"2020-06-04","Yes","2020-09-01","different detail"),
            (1,"2021-01-06",None,  None,       "some detail"),
            (1,"2021-01-08",None,  None,       "some detail"),
            (1,"2021-02-01",None,  None,       "some detail")
        # fmt:on
    ]
    expected_data_specific_case_2 = [
        # fmt:off
            (1,"2020-06-01","Yes","2020-01-01","detail1"),
            (1,"2020-06-02","Yes","2020-01-01","detail1"),
            (1,"2020-06-03","Yes","2020-01-01","detail1"),
            (1,"2020-06-04","Yes","2020-01-01","detail1"),
            (1,"2021-01-06","Yes","2020-01-01","detail1"),
            (1,"2021-01-08","Yes","2020-01-01","detail1"),
            (1,"2021-02-01","Yes","2020-01-01","detail1")
        # fmt:on
    ]
    schema = """
            participant_id integer,
            visit_date string,
            event_indicator string,
            event_date string,
            detail string"""
            
    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    input_df_2 =  spark_session.createDataFrame(data=input_data_specific_case_1, schema=schema)
    input_df_3 =  spark_session.createDataFrame(data=input_data_specific_case_2, schema=schema)

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data_specific_case_1, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data_specific_case_2, schema=schema)


    output_df_1 = fill_forward_event(
        df=input_df,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        even_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
    )

    output_df_2 = fill_forward_event(
        df=input_df_2,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        even_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
    )

    output_df_3 = fill_forward_event(
        df=input_df_3,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        even_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
    )

    assert_df_equality(output_df_1, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
    assert_df_equality(output_df_2, expected_data_specific_case_1, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
    assert_df_equality(output_df_3, expected_data_specific_case_2, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
