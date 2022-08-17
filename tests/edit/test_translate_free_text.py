import os as os
import pdb
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from chispa import assert_df_equality

from cishouseholds.edit import update_from_lookup_df
from cishouseholds.pipeline.high_level_transformations import export_responses_to_be_translated
from cishouseholds.pipeline.high_level_transformations import get_free_text_responses_to_be_translated
from cishouseholds.pipeline.high_level_transformations import transform_translated_responses_into_lookup

# from cishouseholds.derive import translate_column_regex_replace
# from cishouseholds.pipeline.high_level_transformations import update_free_text_responses_from_lookup


def test_export_responses_to_be_translated(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id5", "win5", "Welsh",    "Test text","Test text"),    #id5 is testing english only choices - this is beyond the scope of expected data
            ("id6", "win6", "Welsh",    "Test text",None       ),
            # id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="participant_id string, \
                participant_completion_window_id string, \
                form_language string, \
                free_text_1 string, \
                free_text_2 string",
    )

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("Test text", None),
            (None, None),
            ("id6win6", None),
            # id5 is testing english only choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="original string, \
                translated string",
    )
    # import pdb; pdb.set_trace()

    output_df = export_responses_to_be_translated(input_df)
    output_df = spark_session.createDataFrame(data=output_df, schema="original string, translated string")

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
    )


def test_transform_translated_responses_into_lookup(spark_session):
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id5win5",  None, "free_text_1",   "Test text",    "Translated text"  ),      #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id5win5",  None, "form_language", "Welsh",        "Translated"  ),
            # id1/win1 is testing english only choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="id string, \
                dataset_name string, \
                target_column_name string, \
                old_value string, \
                new_value string",
    )

    # import pdb; pdb.set_trace()
    output_df = transform_translated_responses_into_lookup(spark_session)

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
    )


def test_replace_free_text_responses_from_lookup(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id1", "win1", "Welsh",        "Test text","Test text","Yes",  "No" ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id2", "win1", "Welsh",        None,       "Test text","No",   "Yes"),    #id2/win1 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id2", "win2", "Welsh",        "Test text","Test text","Yes",  "No" ),    #id2/win2 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id3", "win3", "Welsh",        "Test text",None       ,"Yes",  "Yes"),    #id3/win3 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id4", "win4", "Welsh",        None       ,"Test text","No",   "No" ),    #id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id5", "win5", "Welsh",        None       ,"Test text","Yes",  "No" ),    #id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id6", "win6", "Welsh",        "Test text","Test text","Yes",  "No" ),
            # id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="participant_id string, \
                participant_completion_window_id string, \
                form_language string, \
                free_text_1 string, \
                free_text_2 string, \
                fixed_var_1 string, \
                fixed_var_2 string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id1", "win1", "Translated",   "New text", "Test text","Yes",  "No" ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id2", "win1", "Translated",   None,       "New text" ,"No",   "Yes"),    #id2/win1 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id2", "win2", "Translated",   "New text", "New text" ,"Yes",  "No" ),    #id2/win2 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id3", "win3", "Translated",   "New text", None       ,"Yes",  "Yes"),    #id3/win3 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id4", "win4", "Translated",   None,       "New text" ,"No",   "No" ),    #id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id5", "win5", "Welsh",        None       ,"Test text","Yes",  "No" ),    #id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id6", "win6", "Welsh",        "Test text","Test text","Yes",  "No" ),
            # id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="participant_id string, \
                participant_completion_window_id string, \
                form_language string, \
                free_text_1 string, \
                free_text_2 string, \
                fixed_var_1 string, \
                fixed_var_2 string",
    )

    test_lookup_df_long = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id1win1",  None, "free_text_1",   "Test text",    "New text"  ),      #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id2win1",  None, "free_text_2",   "Test text",    "New text"  ),      #id2/win1 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id2win2",  None, "free_text_1",   "Test text",    "New text"  ),      #id2/win2 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id2win2",  None, "free_text_2",   "Test text",    "New text"  ),      #id2/win2 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id3win3",  None, "free_text_1",   "Test text",    "New text"  ),      #id3/win3 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id4win4",  None, "free_text_2",   "Test text",    "New text"  ),      #id4/win4 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id1win1",  None, "form_language", "Welsh",        "Translated"  ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id2win1",  None, "form_language", "Welsh",        "Translated"  ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id2win2",  None, "form_language", "Welsh",        "Translated"  ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id3win3",  None, "form_language", "Welsh",        "Translated"  ),    #id1/win1 is testing english only choices - this is beyond the scope of expected data
            ("id4win4",  None, "form_language", "Welsh",        "Translated"  ),
            # id1/win1 is testing english only choices - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="id string, \
                dataset_name string, \
                target_column_name string, \
                old_value string, \
                new_value string",
    )

    # import pdb; pdb.set_trace()
    input_df = input_df.withColumn(
        "id", F.concat(F.lit(F.col("participant_id")), F.lit(F.col("participant_completion_window_id")))
    )
    output_df = update_from_lookup_df(
        df=input_df,
        lookup_df=test_lookup_df_long,
        id_column="id",
    )

    assert_df_equality(
        output_df.drop("id"),
        expected_df,
        ignore_row_order=False,
        ignore_column_order=False,
    )


def test_get_responses_to_be_translated(spark_session):
    input_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            ("id1", "win1", "Welsh",        None,       None,       "Yes",  "No"),  #id1 is testing special characters
            ("id2", "win2", "Welsh",        None,       None,       "Yes",  "No"),  #id2 is testing special characters
            ("id3", "win3", "English",      None,       None,       "Yes",  "No"),  #id3 id testing single responses
            ("id4", "win4", "English",      None,       None,       "Yes",  "No"),  #id4 is testing null responses
            ("id5", "win5", "Welsh",        "Test text","Test text",None,   None),  #id5 is testing english only choices - this is beyond the scope of expected data
            ("id6", "win6", "Welsh",        "Test text",None,       None,   None),  #id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id7", "win7", "Welsh",        None,       None,       None,   None),  #id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id8", "win8", "Translated",   "Test text","Test text",None,   None),  #id7 is testing erroneus blank entry with space - this is beyond the scope of expected data
            ("id9", "win9", "Translated",   "Test text",None,       None,   None),  #id8 is testing erroneus blank entry - this is beyond the scope of expected data
            ("id0", "win0", "Translated",   None,       None,       None,   None),
            # id8 is testing erroneus blank entry - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="participant_id string, \
                participant_completion_window_id string, \
                form_language string, \
                free_text_1 string, \
                free_text_2 string, \
                fixed_var_1 string, \
                fixed_var_2 string",
    )
    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,
            #("id1", "win1", "Welsh",    None,       None       ),  #id1 is testing special characters
            #("id2", "win2", "Welsh",    None,       None       ),  #id2 is testing special characters
            #("id3", "win3", "English",  None,       None       ),  #id3 id testing single responses
            #("id4", "win4", "English",  None,       None       ),  #id4 is testing null responses
            ("id5", "win5", "Welsh",    "Test text","Test text"),    #id5 is testing english only choices - this is beyond the scope of expected data
            ("id6", "win6", "Welsh",    "Test text",None       ),
            # id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            # ("id7", "win7", "Welsh",    None,       None       ),    #id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            # ("id8", "win8", "English",  "Test text","Test text"),    #id7 is testing erroneus blank entry with space - this is beyond the scope of expected data
            # ("id9", "win9", "English",  "Test text",None       ),    #id8 is testing erroneus blank entry - this is beyond the scope of expected data
            # ("id0", "win0", "English",  None,       None       ),    #id8 is testing erroneus blank entry - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="participant_id string, \
                participant_completion_window_id string, \
                form_language string, \
                free_text_1 string, \
                free_text_2 string",
    )

    test_unique_id_cols = ["participant_id", "participant_completion_window_id"]
    test_free_text_cols = ["free_text_1", "free_text_2"]

    # import pdb; pdb.set_trace()
    output_df = get_free_text_responses_to_be_translated(
        df=input_df,
        unique_id_cols=test_unique_id_cols,
        free_text_cols=test_free_text_cols,
    )

    assert_df_equality(
        output_df,
        expected_df,
        ignore_row_order=False,
        ignore_column_order=True,
    )

    # import pdb; pdb.set_trace()
    list_of_xlsx_file_paths = [os.path.join(os.getcwd(), _) for _ in os.listdir(os.getcwd()) if _.endswith(".xlsx")]
    list_of_csv_file_paths = [os.path.join(os.getcwd(), _) for _ in os.listdir(os.getcwd()) if _.endswith(".csv")]
    list_of_file_paths = list_of_xlsx_file_paths + list_of_csv_file_paths
    for path in list_of_file_paths:
        os.remove(path)
