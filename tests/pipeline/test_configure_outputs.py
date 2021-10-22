# from chispa import assert_df_equality
from cishouseholds.pipeline.post_merge_processing import configure_outputs


def test_configure_outputs(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            ("England", 6, 2, "02-6SY"),
            ("NI", 9, 5, "02-6SY"),
            ("Scotland", 11, 7, "07SY-11SY"),
            ("Wales", 15, 10, "07SY-11SY"),
            ("Wales", 15, 6, "07SY-11SY"),
            ("Scotland", 15, 6, None),
            ("England", 17, 12, "12SY-24"),
            ("NI", 18, 13, "12SY-24"),
            ("England", 25, 12, "25-34"),
            ("NI", 55, 79, "50-69"),
            ("NI", 88, 1, "70+"),
        ],
        schema="country string, age integer, school_year integer, output string",
    )
    output_df = configure_outputs(
        input_df,
        selection_columns=["country", "age", "school_year"],
        name_map={"school_year": "muppet_show_year"},
        value_map={"output": {"02-6SY": "gibberish"}},
    )
    output_df = output_df
    # assert_df_equality(output_df, expected_df, ignore_nullable=True)
