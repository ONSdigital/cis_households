import unittest

from chispa import assert_df_equality
from pyspark.sql import functions as F

from cishouseholds.weights.pre_calibration import precalibration_checkpoints


def test_precalibration_checkpoints(spark_session):
    schema = """
                country string,
                groupby double,
                sample_group string,
                scaled_dweight_adjusted double,
                dweight_1 double,
                dweight_2 double,
                not_positive_or_null integer
            """
    expected_df_not_pass = spark_session.createDataFrame(
        data=[
            # fmt: off
            #   country           groupby     sample_group   scaled_dweight_adjusted_swab
            #                                                       dweight_1   dweight_2   not_positive_or_null
                ('england',       2.0,       'new',          1.0,    2.5,        1.0,        1),
                ('england',       2.0,       'new',          1.0,    -1.5,       1.2,        None),
                ('england',       2.0,       'new',          1.0,    -1.5,       None,       None),

                ('england',       2.0,       'old',          1.0,    2.5,        1.0,        1),
                ('england',       2.0,       'old',          1.0,    -1.5,       1.2,        None),
                ('england',       2.0,       'old',          1.0,    -1.5,       None,       None),
            # fmt: on
        ],
        schema=schema,
    )
    input_df_not_pass = expected_df_not_pass.drop("not_positive_or_null")

    check_1_message = "check_1dd: The design weights are NOT adding up to total population."

    class TestCase(unittest.TestCase):
        def test(self):
            with self.assertRaises(Exception) as context:
                precalibration_checkpoints(
                    df=input_df_not_pass,
                    country_col="country",
                    grouping_list=["groupby", "sample_group"],
                    scaled_dweight_adjusted="scaled_dweight_adjusted",
                    total_population="groupby",
                    dweight_list=["dweight_1", "dweight_2"],
                )
            self.assertTrue(check_1_message in context.exception)

    import pdb

    pdb.set_trace()

    if __name__ == "__main__":
        unittest.main()

    #
    # assert check_1 is not True
    # assert check_2 is not True
    # assert check_3 is not True
    # assert check_4 is not True

    # expected_df_pass = spark_session.createDataFrame(
    #     data=[
    #         # fmt: off
    #         #   country           groupby     sample_group   scaled_dweight_adjusted_swab
    #         #                                                       dweight_1   dweight_2   not_positive_or_null
    #             ('england',       3.0,       'new',          1.0,   7.0,         3.0,        None),
    #             ('england',       3.0,       'new',          1.0,   7.0,         3.0,        None),
    #             ('england',       3.0,       'new',          1.0,   7.0,         3.0,        None),

    #             ('england',       3.0,       'old',          1.0,   4.0,         5.0,        None),
    #             ('england',       3.0,       'old',          1.0,   4.0,         5.0,        None),
    #             ('england',       3.0,       'old',          1.0,   4.0,         5.0,        None),
    #         # fmt: on
    #     ],
    #     schema=schema,
    # )
    # input_df_pass = expected_df_pass.drop("not_positive_or_null")

    # check_1, check_2, check_3, check_4 = precalibration_checkpoints(
    #     df=input_df_pass,
    #     country_col="country",
    #     grouping_list=["groupby", "sample_group"],
    #     scaled_dweight_adjusted="scaled_dweight_adjusted",
    #     total_population="groupby",
    #     dweight_list=["dweight_1", "dweight_2"],
    # )
    # assert check_1 is True
    # assert check_2 is True
    # assert check_3 is True
    # assert check_4 is True
