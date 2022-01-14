from chispa import assert_df_equality

from cishouseholds.weights.pre_calibration import cutoff_day_to_ever_never
from cishouseholds.weights.pre_calibration import dataset_flag_generation_evernever_OR_longcovid


def test_dataset_generation_ever_never_swab(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2022-02-01',   'negative',   18, None), # latest negative
                (1,     '2022-02-03',   'negative',   18, None),
                (1,     '2022-02-05',   'negative',   18,  1),
                (2,     '2022-02-01',   'negative',   1,  None), # patient below 2 years old
                (3,     '2022-02-01',   'positive',   1,  None), # patient below 2 years but positive (ignore)
                (4,     '2022-02-01',   'negative',   1,  None), # patient below 2 years but negative (ignore)
                (5,     '2022-02-01',   'negative',   18, None),
                (5,     '2022-02-02',   'positive',   18, None),
                (5,     '2022-02-02',   'positive',   18,  1), # repeated case ignore
                (5,     '2022-02-03',   'negative',   18, None),
                (6,     '2022-02-02',   'positive',   20, None),
                (6,     '2022-02-03',   'positive',   20,  1), # repeated case ignore
                (6,     '2022-02-07',   'negative',   20, None),
            # fmt: on
        ],
        schema="""
            patient_id integer,
            visit_date string,
            test_result string,
            age integer,
            ever_never integer
            """,
    )
    input_df = expected_df.drop("ever_never_swab")

    output_df = dataset_flag_generation_evernever_OR_longcovid(
        df=input_df,
        column_test_result="test_result",
        patient_id_column="patient_id",
        visit_date_column="visit_date",
        age_column="age",
        dataset_flag_column="ever_never",
        type_test="swab",
        positive_case="positive",
        negative_case="negative",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)


def test_dataset_generation_ever_never_antibodies(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2021-02-01',   'negative',   18, 1),       # before pass
                (2,     '2021-02-01',   'negative',   10, None),    # before not pass

                (3,     '2022-02-05',   'negative',   9,  1),       # after  pass
                (4,     '2022-02-05',   'negative',   5,  None),
            # after  not pass
            # fmt: on
        ],
        schema="""
            patient_id integer,
            visit_date string,
            test_result string,
            age integer,
            ever_never integer
            """,
    )
    input_df = expected_df.drop("ever_never")

    output_df = dataset_flag_generation_evernever_OR_longcovid(
        df=input_df,
        column_test_result="test_result",
        patient_id_column="patient_id",
        visit_date_column="visit_date",
        age_column="age",
        dataset_flag_column="ever_never",
        type_test="antibodies",
        positive_case="positive",
        negative_case="negative",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)


def test_cutoff_day_to_ever_never(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2022-01-30',   16,     None),  # outside
                (1,     '2022-02-01',   14,     None),  # edge case
                (1,     '2022-02-10',   5,      1),     # inside
                (1,     '2022-02-20',   -5,     None),
            # outside
            # fmt: on
        ],
        schema="""
            patient_id integer,
            visit_date string,
            diff_visit_cutoff string,
            14_days integer
            """,
    )
    input_df = expected_df.drop("diff_visit_cutoff")

    output_df = cutoff_day_to_ever_never(
        df=input_df,
        days=14,
        cutoff_date="2022-02-15",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)


def test_longcovid_dataset(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
                (1,     '2021-02-01',   'no',       18,     None),
                (1,     '2021-02-10',   'yes',      18,     1),
                (1,     '2021-02-20',   'no',       18,     None),

                (2,     '2021-02-01',   'no',       18,     None),
                (2,     '2021-02-10',   'no',       18,     1),
            # fmt: on
        ],
        schema="""
            patient_id integer,
            visit_date string,
            long_covid_have_symptoms string,
            age integer,
            longcovid integer
            """,
    )
    input_df = expected_df.drop("ever_never")

    output_df = dataset_flag_generation_evernever_OR_longcovid(
        df=input_df,
        column_test_result="long_covid_have_symptoms",
        patient_id_column="patient_id",
        visit_date_column="visit_date",
        age_column="age",
        dataset_flag_column="longcovid",
        type_test="antibodies",
        positive_case="yes",
        negative_case="no",
    )
    assert_df_equality(output_df, expected_df, ignore_column_order=True, ignore_row_order=True, ignore_nullable=True)
