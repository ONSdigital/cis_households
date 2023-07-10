from chispa import assert_df_equality

from cishouseholds.impute import fill_forward_target_columns

schema = """
    participant_id integer,
    event_date string,
    event_id integer,
    target_indicator string,
    target_date string,
    detail string
"""


def test_fill_forward_target_columns_1(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-07",1,"A","2020-01-01","some different detail"),
            (1,"2019-12-01",2,"B","2020-01-01","some detail"), # this row is invalid as event_date before event date
            (1,"2020-01-05",3,"C","2020-01-01","some detail"),
            (1,"2020-01-06",4,"D",None,        "some detail"),
            (1,"2020-01-08",5,"E","2020-01-07","some different detail"),
            (1,"2020-02-01",6,"F","2020-02-02","some other detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2019-12-01",2,"No",  None,        None),
            (1,"2020-01-05",3,"Yes","2020-01-01","some detail"),
            (1,"2020-01-06",4,"Yes","2020-01-01","some detail"),
            (1,"2020-01-07",1,"Yes","2020-01-07","some different detail"),
            (1,"2020-01-08",5,"Yes","2020-01-07","some different detail"),
            (1,"2020-02-01",6,"Yes","2020-01-07","some different detail")
        # fmt:on
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_target_columns(
        df=input_df,
        target_indicator_column="target_indicator",
        target_date_column="target_date",
        target_date_tolerance=0,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        event_datetime_column="event_date",
        event_id_column="event_id",
        use_hdfs=False,
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_fill_forward_target_columns_2(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-01-01",1,"No",None,"some detail"),
            (1,"2019-12-02",2,None,None,"some detail"),
            (1,"2020-01-03",3,"No",None,"some detail"),
            (1,"2020-01-04",4,"No",None,"some detail"),
            (1,"2020-01-05",5,None,None,None),
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2020-01-01",1,"No",None,"some detail"),
            (1,"2019-12-02",2,"No",None,"some detail"),
            (1,"2020-01-03",3,"No",None,"some detail"),
            (1,"2020-01-04",4,"No",None,"some detail"),
            (1,"2020-01-05",5,"No",None,None),
        # fmt:on
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_target_columns(
        df=input_df,
        target_indicator_column="target_indicator",
        target_date_column="target_date",
        target_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        event_datetime_column="event_date",
        event_id_column="event_id",
        use_hdfs=False,
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_fill_forward_target_columns_3(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-06-01",1,"No", "2020-01-03","detail1"),
            (1,"2020-06-02",2,"Yes","2020-01-02","detail2"),
            (1,"2020-06-03",3,"Yes","2020-01-01","detail3"),
            (1,"2020-06-04",4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,None,  None,       "some detail"),
            (1,"2021-01-08",6,None,  None,       "some detail"),
            (1,"2021-02-01",7,None,  None,       "some detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2020-06-01",1,"Yes","2020-01-03","detail1"),
            (1,"2020-06-02",2,"Yes","2020-01-03","detail1"),
            (1,"2020-06-03",3,"Yes","2020-01-03","detail1"),
            (1,"2020-06-04",4,"Yes","2020-01-03","detail1"),
            (1,"2021-01-06",5,"Yes","2020-01-03","detail1"),
            (1,"2021-01-08",6,"Yes","2020-01-03","detail1"),
            (1,"2021-02-01",7,"Yes","2020-01-03","detail1")
        # fmt:on
    ]
    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_target_columns(
        df=input_df,
        target_indicator_column="target_indicator",
        target_date_column="target_date",
        target_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        event_datetime_column="event_date",
        event_id_column="event_id",
        use_hdfs=False,
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_fill_forward_target_columns_4(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-06-02",1,"No", "2020-01-01","detail1"),
            (1,"2020-06-01",2,"Yes","2020-01-02","detail2"),
            (1,None,        3,"Yes","2020-01-03","detail3"),
            (1,None,        4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,None,  None,       "some detail"),
            (1,"2021-01-08",6,None,  None,       "some detail"),
            (1,"2021-02-01",7,None,  None,       "some detail")
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2020-06-02",1,"Yes","2020-01-02","detail2"),
            (1,"2020-06-01",2,"Yes","2020-01-02","detail2"),
            (1,None,        3,"Yes","2020-01-03","detail3"), # nothing happens to this row as no event_datetime
            (1,None,        4,"Yes","2020-09-01","different detail"),
            (1,"2021-01-06",5,"Yes","2020-01-02","detail2"),
            (1,"2021-01-08",6,"Yes","2020-01-02","detail2"),
            (1,"2021-02-01",7,"Yes","2020-01-02","detail2")
        # fmt:on
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_target_columns(
        df=input_df,
        target_indicator_column="target_indicator",
        target_date_column="target_date",
        target_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        event_datetime_column="event_date",
        event_id_column="event_id",
        use_hdfs=False,
    )

    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)


def test_fill_forward_target_columns_5(spark_session):
    input_data = [
        # fmt:off
            (1,"2020-10-02",1,"A","2020-03-01","some different detail"),
            (1,"2020-11-05",2,"B",None,         None),
            (1,"2020-05-15",3,"C","2020-03-06","some detail"),
            (1,"2020-05-22",4,"D","2020-03-09", None),
            (1,"2021-10-02",5,"E","2021-03-01","some different detail 2"),
            (1,"2021-11-05",6,"F",None,         None),
            (1,"2021-05-15",7,"G","2021-03-06","some detail 2"),
            (1,"2021-05-22",8,"H","2021-03-09", None),
        # fmt:on
    ]
    expected_data = [
        # fmt:off
            (1,"2020-10-02",1,"Yes","2020-03-06","some detail"),
            (1,"2020-11-05",2,"Yes","2020-03-06","some detail"),
            (1,"2020-05-15",3,"Yes","2020-03-06","some detail"),
            (1,"2020-05-22",4,"Yes","2020-03-06","some detail"),
            (1,"2021-10-02",5,"Yes","2021-03-06","some detail 2"),
            (1,"2021-11-05",6,"Yes","2021-03-06","some detail 2"),
            (1,"2021-05-15",7,"Yes","2021-03-06","some detail 2"),
            (1,"2021-05-22",8,"Yes","2021-03-06","some detail 2"),
        # fmt:on
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=schema)
    expected_df = spark_session.createDataFrame(data=expected_data, schema=schema)

    output_df = fill_forward_target_columns(
        df=input_df,
        target_indicator_column="target_indicator",
        target_date_column="target_date",
        target_date_tolerance=7,
        detail_columns=["detail"],
        participant_id_column="participant_id",
        event_datetime_column="event_date",
        event_id_column="event_id",
        use_hdfs=False,
    )
    assert_df_equality(output_df, expected_df, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
