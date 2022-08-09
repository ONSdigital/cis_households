from chispa import assert_df_equality

from cishouseholds.derive import translate_column_regex_replace


def test_translate_column_regex_replace(spark_session):

    expected_df = spark_session.createDataFrame(
        data=[
            # fmt: off
            # input,                                                                    # expected
            ("id1", "Byddai'n well gen i beidio â dweud;Blwch sampl;Plasteri;Lawnsedi", "Prefer not to say;Sample box;Plasters;Lancets"),       #id1 is testing special characters
            ("id2", "Byddai'n well gen i beidio â dweud;Lawnsedi",                      "Prefer not to say;Lancets"),                           #id2 is testing special characters
            ("id3", "Lawnsedi",                                                         "Lancets"),                                             #id3 id testing single responses
            ("id4", None,                                                               None),                                                  #id4 is testing null responses
            ("id5", "Prefer not to say;Sample box;Plasters;Lancets",                    "Prefer not to say;Sample box;Plasters;Lancets"),       #id5 is testing english only choices - this is beyond the scope of expected data
            ("id6", "Prefer not to say;Lawnsedi;Plasteri",                              "Prefer not to say;Lancets;Plasters"),                  #id6 is testing mixed english and welsh choices - this is beyond the scope of expected data
            ("id7", "Prefer not to say; ;Lawnsedi",                                     "Prefer not to say; ;Lancets"),                         #id7 is testing erroneus blank entry with space - this is beyond the scope of expected data
            ("id8", "Prefer not to say;;Lawnsedi",                                      "Prefer not to say;;Lancets"),
            # id8 is testing erroneus blank entry - this is beyond the scope of expected data
            # fmt: on
        ],
        schema="uid string, reference_choices string, expected_choices string",
    )

    mapping_dict = {
        "Byddai'n well gen i beidio â dweud": "Prefer not to say",
        "Lawnsedi": "Lancets",
        "Plasteri": "Plasters",
        "Blwch sampl": "Sample box",
    }

    # import pdb; pdb.set_trace()
    output_df = translate_column_regex_replace(
        df=expected_df.drop("expected_choices"),
        reference_column="reference_choices",
        multiple_choice_dict=mapping_dict,
    )

    assert_df_equality(
        output_df.select("uid", "reference_choices").withColumnRenamed("reference_choices", "translated_choices"),
        expected_df.select("uid", "expected_choices").withColumnRenamed("expected_choices", "translated_choices"),
        ignore_row_order=False,
        ignore_column_order=True,
    )
