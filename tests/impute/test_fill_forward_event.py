from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_event


def test_fill_forward_event(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-07",1,"A","2020-01-01","some different detail"),
            (1,"2019-12-01",2,"B","2020-01-01","some detail"),
            (1,"2020-01-05",3,"C","2020-01-01","some detail"),
            (1,"2020-01-06",4,"D",None,        "some detail"),
            (1,"2020-01-08",5,"E","2020-01-07","some different detail"),
            (1,"2020-02-01",6,"F","2020-02-02","some other detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2019-12-01",1,"No",  None,        None),
            (1,"2020-01-05",2,"Yes","2020-01-01","some detail"),
            (1,"2020-01-06",3,"Yes","2020-01-01","some detail"),
            (1,"2020-01-07",4,"Yes","2020-01-07","some different detail"),
            (1,"2020-01-08",5,"Yes","2020-01-07","some different detail"),
            (1,"2020-02-01",6,"Yes","2020-01-07","some different detail")
        # fmt:on
    ]
    input_data_specific_case_1 = [
        # fmt:off
            (1,"2020-01-01",1,"No",None,"some detail"),
            (1,"2019-12-02",2,None,None,"some detail"),
            (1,"2020-01-03",3,"No",None,"some detail"),
            (1,"2020-01-04",4,"No",None,"some detail"),
            (1,"2020-01-05",5,None,None,None),
        # fmt:on
    ]
    expected_data_specific_case_1 = [
        # fmt:off
            (1,"2020-01-01",1,"No",None,"some detail"),
            (1,"2019-12-02",2,"No",None,"some detail"),
            (1,"2020-01-03",3,"No",None,"some detail"),
            (1,"2020-01-04",4,"No",None,"some detail"),
            (1,"2020-01-05",5,"No",None,None),
        # fmt:on
    ]
    input_data_specific_case_2 = [
        # fmt:off
            (1,"2020-06-01",1,"No", "2020-01-01","detail1"),
            (1,"2020-06-02",2,"Yes","2020-01-02","detail2"),
            (1,"2020-06-03",3,"Yes","2020-01-03","detail3"),
            (1,"2020-06-04",4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,None,  None,       "some detail"),
            (1,"2021-01-08",6,None,  None,       "some detail"),
            (1,"2021-02-01",7,None,  None,       "some detail")
        # fmt:on
    ]
    expected_data_specific_case_2 = [
        # fmt:off
            (1,"2020-06-01",1,"Yes","2020-01-01","detail1"),
            (1,"2020-06-02",2,"Yes","2020-01-01","detail1"),
            (1,"2020-06-03",3,"Yes","2020-01-01","detail1"),
            (1,"2020-06-04",4,"Yes","2020-01-01","detail1"),
            (1,"2021-01-06",5,"Yes","2020-01-01","detail1"),
            (1,"2021-01-08",6,"Yes","2020-01-01","detail1"),
            (1,"2021-02-01",7,"Yes","2020-01-01","detail1")
        # fmt:on
    ]
    input_data_specific_case_3 = [
        # fmt:off
            (1,"2020-06-01",1,"No", "2020-01-01","detail1"),
            (1,"2020-06-02",1,"Yes","2020-01-02","detail2"),
            (1,None,3,"Yes","2020-01-03","detail3"),
            (1,None,4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,None,  None,       "some detail"),
            (1,"2021-01-08",6,None,  None,       "some detail"),
            (1,"2021-02-01",7,None,  None,       "some detail")
        # fmt:on
    ]
    expected_data_specific_case_3 = [
        # fmt:off
            (1,"2020-06-01",1,"Yes","2020-01-01","detail1"),
            (1,"2020-06-02",1,"Yes","2020-01-01","detail1"),
            (1,None,3,"Yes","2020-01-03","detail3"),
            (1,None,4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,"Yes","2020-01-01","detail1"),
            (1,"2021-01-08",6,"Yes","2020-01-01","detail1"),
            (1,"2021-02-01",7,"Yes","2020-01-01","detail1")
        # fmt:on
    ]
    schema = """
            participant_id integer,
            visit_date string,
            visit_id integer,
            event_indicator string,
            event_date string,
            detail string"""

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    input_df_2 = spark_session.createDataFrame(data=input_data_specific_case_1, schema=schema)
    input_df_3 = spark_session.createDataFrame(data=input_data_specific_case_2, schema=schema)
    input_df_4 = spark_session.createDataFrame(data=input_data_specific_case_3, schema=schema)

    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)
    expected_df_specific_case_1 = spark_session.createDataFrame(data=expected_data_specific_case_1, schema=schema)
    expected_df_specific_case_2 = spark_session.createDataFrame(data=expected_data_specific_case_2, schema=schema)
    expected_df_specific_case_3 = spark_session.createDataFrame(data=expected_data_specific_case_3, schema=schema)

    output_df_1 = fill_forward_event(
        df=input_df,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        event_date_tolerance=0,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
        visit_id_column="visit_id",
    )

    output_df_2 = fill_forward_event(
        df=input_df_2,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        event_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
        visit_id_column="visit_id",
    )

    output_df_3 = fill_forward_event(
        df=input_df_3,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        event_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
        visit_id_column="visit_id",
    )

    output_df_4 = fill_forward_event(
        df=input_df_4,
        event_indicator_column="event_indicator",
        event_date_column="event_date",
        event_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        visit_datetime_column="visit_date",
        visit_id_column="visit_id",
    )

    assert_df_equality(output_df_1, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
    assert_df_equality(
        output_df_2, expected_df_specific_case_1, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
    assert_df_equality(
        output_df_3, expected_df_specific_case_2, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
    assert_df_equality(
        output_df_4, expected_df_specific_case_3, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True
    )
