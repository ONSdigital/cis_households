from chispa import assert_df_equality

from cishouseholds.derive import assign_n_participants_corrected


def test_assign_nametestassign_n_participants_corrected(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            (1, 5, 2, 3, 4, 6, 10),
            (1, 11, 2, 3, 4, 6, 16),
        ],
        schema="n_participants integer, non_consent integer, infant_1_age integer, infant_2_age integer, infant_31_age integer, notpresent_11_age integer, total integer",
    )
    output_df = assign_n_participants_corrected(expected_df.drop("total"), "total", "n_participants", "non_consent")
    assert_df_equality(output_df, expected_df)
