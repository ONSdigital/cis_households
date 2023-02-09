from chispa import assert_df_equality

from cishouseholds.edit import update_work_main_job_changed


# def test_update_work_main_job_changed(spark_session):
#     schema = """
#         id integer,
#         a integer,
#         b integer,
#         c integer,
#         d integer,
#         changed string
#         """
#     input_df = spark_session.createDataFrame(
#         # fmt: off
#         data=[
#             (1, None, None, None,  1, None),
#             (1, 1,    0,    0,     1, "No"),
#             (1, 1,    2,    0,     2, None),
#             (1, 1,    2,    0,     None, "No"),
#             (1, None, None, None,  3, "Yes"),
#             (1, None, None, None,  5, "No")
#         ],
#         # fmt: on
#         schema=schema
#     )

#     expected_df = spark_session.createDataFrame(
#         # fmt: off
#         data=[
#             (1, None, None, None, 1, "Yes"), # first row and none null response
#             (1, 1,    0,    0,    1, "Yes"), # d hasn't changed
#             (1, 1,    2,    0,    2, "Yes"),
#             (1, 1,    2,    0,    None, "Yes"), # only d has changed
#             (1, None, None, None, 3, "Yes"),
#             (1, None, None, None, 5, "Yes")
#         ],
#         # fmt: on
#         schema=schema
#     )

#     expected_df_2 = spark_session.createDataFrame(
#         # fmt: off
#         data=[
#             (1, None, None, None, 1, "No"), # first row and none null response
#             (1, 1,    0,    0,    1, "No"), # d hasn't changed
#             (1, 1,    2,    0,    2, "No"),
#             (1, 1,    2,    0,    None, "No"), # only d has changed
#             (1, None, None, None, 3, "No"),
#             (1, None, None, None, 5, "No")
#         ],
#         # fmt: on
#         schema=schema
#     )

#     output_df = update_work_main_job_changed(
#         df=input_df,
#         column_name_to_update="changed",
#         participant_id_column="id",
#         change_to_any_columns=["d"],
#         change_to_not_null_columns=["a", "b", "c"],
#     )

#     assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)


def test_update_work_main_job_changed(spark_session):
    schema = """
        id integer,
        a integer,
        b integer,
        c integer,
        d integer,
        e integer,
        f integer,
        g integer,
        changed string
        """
    input_df = spark_session.createDataFrame(
        # fmt: off
        data=[
            ("Yes", "Delivery of petrol", "Petroleum driver", None, None, "No", None, None),
            ("Yes", None, "truck driver", None, None, "No", None, None),
            ("Yes", "Driving", "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", "delivering fuel", "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", None, "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", "Delivering fuel", "Field delivery driver", None, None, "No", None, None),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("Yes", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("Yes", None, None, None, None, None, None, None),
            ("Yes", None, None, None, None, None, None, "No"),
            ("Yes", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, "Yes"),
            ("Yes", None, None, None, None, None, None, "Yes"),
            ("No", None, None, None, None, None, None, "No"),
            ("No", None, None, None, None, None, None, "No"),
            ("Yes", None, None, None, None, None, None, "No"),
            ("No", None, None, None, None, None, None, "Yes"),
        ],
        # fmt: on
        schema=schema
    )

    expected_df = spark_session.createDataFrame(
        # fmt: off
       data=[
            ("Yes", "Delivery of petrol", "Petroleum driver", None, None, "No", None, None),
            ("Yes", None, "truck driver", None, None, "No", None, None),
            ("Yes", "Driving", "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", "delivering fuel", "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", None, "Fuel delivery driver", None, None, "No", None, None),
            ("Yes", "Delivering fuel", "Field delivery driver", None, None, "No", None, None),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("Yes", "Deliver fuel", "HGV Driver", None, None, "No", "No", "No"),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, None),
            ("Yes", None, None, None, None, None, None, None),
            ("Yes", None, None, None, None, None, None, "No"),
            ("Yes", None, None, None, None, None, None, None),
            ("No", None, None, None, None, None, None, "Yes"),
            ("Yes", None, None, None, None, None, None, "Yes"),
            ("No", None, None, None, None, None, None, "No"),
            ("No", None, None, None, None, None, None, "No"),
            ("Yes", None, None, None, None, None, None, "No"),
            ("No", None, None, None, None, None, None, "Yes"),
        ],
        # fmt: on
        schema=schema
    )

    output_df = update_work_main_job_changed(
        df=input_df,
        column_name_to_update="changed",
        participant_id_column="id",
        change_to_any_columns=["f", "g"],
        change_to_not_null_columns=["a", "b", "c", "d", "e", "f"],
    )

    assert_df_equality(output_df, expected_df, ignore_nullable=True, ignore_row_order=True)
