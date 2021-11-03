from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import cishouseholds.merge as M
from cishouseholds.compare import prepare_for_union
from cishouseholds.validate import validate_merge_logic


def merge_process_preparation(
    survey_df: DataFrame,
    labs_df: DataFrame,
    merge_type: str,
    barcode_column_name: str,
    visit_date_column_name: str,
    received_date_column_name: str,
) -> DataFrame:
    """
    Common function to either Swab/Antibody merge that executes the functions for assigning an unique identifier
    and count occurrence to decide whether it is 1 to many, many to one, or many to many.

    Parameters
    ----------
    survey_df
        iqvia dataframe
    labs_df
        swab or antibody unmerged dataframe
    merge_type
        either swab or antibody, no other value accepted.
    barcode_column_name
    visit_date_column_name
    received_date_column_name

    Note
    ----
    It is assumed that the barcode column name for survey and labs is the same.
    """
    # survey_df = M.assign_unique_identifier_column(
    #    survey_df, "unique_participant_response_id", ordering_columns=[barcode_column_name]
    # )
    survey_df = M.assign_count_of_occurrences_column(survey_df, barcode_column_name, "count_barcode_voyager")

    # labs_df = M.assign_unique_identifier_column(
    #    labs_df, "unique_id_" + merge_type, ordering_columns=[barcode_column_name]
    # )
    labs_df = M.assign_count_of_occurrences_column(labs_df, barcode_column_name, "count_barcode_" + merge_type)

    outer_df = M.join_dataframes(survey_df, labs_df, barcode_column_name, "outer")

    if merge_type == "swab":
        interval_upper_bound = 480
    elif merge_type == "antibody":
        interval_upper_bound = 240

    if merge_type == "swab":
        interval_upper_bound = 480
    elif merge_type == "antibody":
        interval_upper_bound = 240

    outer_df = M.assign_time_difference_and_flag_if_outside_interval(
        df=outer_df,
        column_name_outside_interval_flag="out_of_date_range_" + merge_type,
        column_name_time_difference="diff_vs_visit_hr",
        start_datetime_reference_column=visit_date_column_name,
        end_datetime_reference_column=received_date_column_name,
        interval_lower_bound=-24,
        interval_upper_bound=interval_upper_bound,
        interval_bound_format="hours",
    )
    outer_df = M.assign_absolute_offset(
        df=outer_df, column_name_to_assign="abs_offset_diff_vs_visit_hr", reference_column="diff_vs_visit_hr", offset=24
    )

    return assign_merge_process_group_flags_and_filter(df=outer_df, merge_type=merge_type)


def assign_merge_process_group_flags_and_filter(df: DataFrame, merge_type: str):
    """ 
    Assign all merge process group flag columns simultaneously and create separate
    into individual dataframes
    Parameters
    ----------
    df
    merge_types
    """
    match_types = {"mtom": [">1", ">1"], "1tom": ["==1", ">1"], "mto1": [">1", "==1"]}
    for name, condition in match_types.items():
        df = M.assign_merge_process_group_flag(
            df=df,
            column_name_to_assign=name,
            out_of_date_range_flag="out_of_date_range_" + merge_type,
            count_barcode_labs_column_name="count_barcode_" + merge_type,
            count_barcode_labs_condition=condition[0],
            count_barcode_voyager_column_name="count_barcode_voyager",
            count_barcode_voyager_condition=condition[1],
        )
    many_to_many_df = df.filter(F.col("mtom") == 1)
    one_to_many_df = df.filter(F.col("1tom") == 1)
    many_to_one_df = df.filter(F.col("mto1") == 1)
    one_to_one_df = df.filter((F.col("mtom").isNull()) & (F.col("1tom").isNull()) & (F.col("mto1").isNull()))

    # validate that only one match type exists at this point

    return many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df


def merge_process_validation(outer_df: DataFrame, merge_type: str, barcode_column_name: str) -> DataFrame:
    """
    Once the merging, preparation and one_to_many, many_to_one and many_to_many functions are executed,
    this function will apply the validation function after identifying what type of merge it is.
    Parameters
    ----------
    outer_df
    merge_type
        either swab or antibody, no other value accepted.
    barcode_column_name
    """
    merge_type_list = ["1tom", "mto1", "mtom"]
    count_barcode_labs_condition = ["==1", ">1", ">1"]
    count_barcode_voyager_condition = [">1", "==1", ">1"]

    for element in zip(merge_type_list, count_barcode_labs_condition, count_barcode_voyager_condition):
        outer_df = M.assign_merge_process_group_flag(
            df=outer_df,
            column_name_to_assign=element[0] + "_" + merge_type,
            out_of_date_range_flag="out_of_date_range_" + merge_type,
            count_barcode_labs_column_name="count_barcode_" + merge_type,
            count_barcode_labs_condition=element[1],
            count_barcode_voyager_column_name="count_barcode_voyager",
            count_barcode_voyager_condition=element[2],
        )
    flag_column_names = ["drop_flag_1tom_", "drop_flag_mto1_", "drop_flag_mtom_"]
    flag_column_names_syntax = [element + merge_type for element in flag_column_names]

    failed_column_names = ["failed_1tom_", "failed_mto1_", "failed_mtom_"]
    failed_column_names_syntax = [element + merge_type for element in failed_column_names]

    match_type_colums_syntax = [element + "_" + merge_type for element in merge_type_list]

    outer_df = validate_merge_logic(
        df=outer_df,
        flag_column_names=flag_column_names_syntax,
        failed_column_names=failed_column_names_syntax,
        match_type_columns=match_type_colums_syntax,
        group_by_column=barcode_column_name,
    )
    return outer_df


def execute_merge_specific_swabs(
    survey_df: DataFrame,
    labs_df: DataFrame,
    barcode_column_name: str,
    visit_date_column_name: str,
    received_date_column_name: str,
    void_value: str = "void",
) -> DataFrame:
    """
    Specific high level function to execute the process for swab.
    Parameters
    ----------
    survey_df
    labs_df
    barcode_column_name
    visit_date_column_name
    received_date_column_name
    void_value
        by default is "void" but it is possible to specify what is considered as void in the PCR result test.
    """
    merge_type = "swab"
    many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df = merge_process_preparation(
        survey_df=survey_df,
        labs_df=labs_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
        visit_date_column_name=visit_date_column_name,
        received_date_column_name=received_date_column_name,
    )
    window_columns = [
        "abs_offset_diff_vs_visit_hr",
        "diff_vs_visit_hr",
        visit_date_column_name,
        # 4th here is uncleaned barcode from labs (used in the Stata pipeline)
    ]
    print("1 to m input -->")
    one_to_many_df.show()
    one_to_many_df = M.one_to_many_swabs(
        df=one_to_many_df,
        out_of_date_range_flag="out_of_date_range_" + merge_type,
        count_barcode_labs_column_name="count_barcode_" + merge_type,
        count_barcode_voyager_column_name="count_barcode_voyager",
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        pcr_result_column_name="pcr_result_classification",
        void_value=void_value,
        flag_column_name="drop_flag_1tom_" + merge_type,
    )
    print("1 to m output -->")
    one_to_many_df.show()

    print("m to 1 input -->")
    many_to_one_df.show()
    many_to_one_df = M.many_to_one_swab_flag(
        df=many_to_one_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
    )
    print("m to 1 output -->")
    many_to_one_df.show()

    print("m to m input -->")
    many_to_many_df.show()
    many_to_many_df = M.many_to_many_flag(  # UPDATE: window column correct ordering
        df=many_to_many_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=[
            "abs_offset_diff_vs_visit_hr",
            "diff_vs_visit_hr",
            "unique_participant_response_id",
            "unique_pcr_test_id",
        ],
        process_type="swab",
        failed_flag_column_name_to_assign="failed_flag_mtom_" + merge_type,
    )
    print("m to m output -->")
    many_to_many_df.show()

    outer_df = M.union_multiple_tables(tables=[many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df])

    outer_df = merge_process_validation(
       outer_df=outer_df,
       merge_type="swab",
       barcode_column_name=barcode_column_name,
    )

    print("main output --> ")
    outer_df.show()
    return outer_df


def execute_merge_specific_antibody(
    survey_df: DataFrame,
    labs_df: DataFrame,
    barcode_column_name: str,
    visit_date_column_name: str,
    received_date_column_name: str,
) -> DataFrame:
    """
    Specific high level function to execute the process for Antibody.
    Parameters
    ----------
    survey_df
    labs_df
    barcode_column_name
    visit_date_column_name
    received_date_column_name
    """
    merge_type = "antibody"
    many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df = merge_process_preparation(
        survey_df=survey_df,
        labs_df=labs_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
        visit_date_column_name=visit_date_column_name,
        received_date_column_name=received_date_column_name,
    )
    print("1 to m input -->")
    one_to_many_df.show()
    one_to_many_df = M.one_to_many_antibody_flag(
        df=one_to_many_df,
        column_name_to_assign="drop_flag_1tom_" + merge_type,
        group_by_column=barcode_column_name,
        diff_interval_hours="diff_vs_visit_hr",
        siemens_column="assay_siemens",
        tdi_column="antibody_test_result_classification",
        visit_date=visit_date_column_name,
        out_of_date_range_column="out_of_date_range_" + merge_type,
        count_barcode_voyager_column_name="count_barcode_voyager",
        count_barcode_labs_column_name="count_barcode_" + merge_type,
    )
    print("1 to m output -->")
    one_to_many_df.show()

    print("m to 1 input -->")
    many_to_one_df.show()
    many_to_one_df = M.many_to_one_antibody_flag(
        df=many_to_one_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
    )
    print("m to 1 output -->")
    many_to_one_df.show()

    window_columns = [
        "abs_offset_diff_vs_visit_hr",
        "diff_vs_visit_hr",
        "unique_participant_response_id",
        "unique_antibody_test_id",
    ]
    print("m to m input -->")
    many_to_many_df.show()
    many_to_many_df = M.many_to_many_flag(
        df=many_to_many_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        process_type=merge_type,
        failed_flag_column_name_to_assign="failed_flag_mtom_" + merge_type,
    )
    print("m to m output -->")
    many_to_many_df.show()

    outer_df = M.union_multiple_tables(tables=[many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df])
    outer_df = merge_process_validation(
       outer_df,
       merge_type=merge_type,
       barcode_column_name=barcode_column_name,
    )

    print("main output --> ")
    outer_df.show()

    return outer_df


def merge_process_filtering(
    df: DataFrame,
    merge_type: str,
    barcode_column_name: str,
    lab_columns_list: List[str],
    merge_combination: List[str] = ["1tom", "mto1", "mtom"],
    drop_list_columns: List[str] = [],
) -> DataFrame:
    """
    Final filtering process of merging generating successful merges ...
    Parameters
    ----------
    df
        input dataframe with drop, merge_type and failed to merge columns
    merge_type
        either swab or antibody, anything else will fail.
    lab_columns_list
        lab columns to be dropped to be fed back to iqvia records
    merge_combination
        only elements in the list accepted 1tom, mto1, mtom
    drop_list_columns
        present in a list the exact name of columns to be dropped for the final
        3 dataframes df_best_match, df_not_best_match, df_failed_records
    Notes: this function will return 2 dataframes, one with best match records
    another one with not best matched records
    """
    # STEP 1 - RESOLVE FLAGS ------------------------
    # include: failed_flag_mtom_swab

    df = (
        df.withColumn("best_match", F.lit(None).cast("integer"))
        .withColumn("not_best_match", F.lit(None).cast("integer"))
        .withColumn("failed_match", F.lit(None).cast("integer"))
    )

    df = df.withColumn(
        "not_best_match", F.when(F.col(f"out_of_date_range_{merge_type}") == 1, 1).otherwise(F.col("not_best_match"))
    )
    df = df.withColumn(
        "best_match",
        F.when(
            (F.col(f"out_of_date_range_{merge_type}").isNull())
            & (F.col(f"1tom_{merge_type}").isNull())
            & (F.col(f"mto1_{merge_type}").isNull())
            & (F.col(f"mtom_{merge_type}").isNull()),
            1,
        ).otherwise(F.col("best_match")),
    )
    for xtox in merge_combination:
        best_match_logic = (
            (F.col(xtox + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + xtox + "_" + merge_type).isNull())
            & (F.col("failed_" + xtox + "_" + merge_type).isNull())
        )
        not_best_match_logic = (
            (F.col(xtox + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + xtox + "_" + merge_type) == 1)
            & (F.col("failed_" + xtox + "_" + merge_type).isNull())
        )
        failed_match_logic = (F.col(xtox + "_" + merge_type) == 1) & (F.col("failed_" + xtox + "_" + merge_type) == 1)

        df = df.withColumn("best_match", F.when(best_match_logic, 1).otherwise(F.col("best_match")))
        df = df.withColumn("not_best_match", F.when(not_best_match_logic, 1).otherwise(F.col("not_best_match")))
        df = df.withColumn("failed_match", F.when(failed_match_logic, 1).otherwise(F.col("failed_match")))  # \
        # .drop(*drop_list_columns)

    if merge_type == "swab":
        df = df.withColumn(
            "failed_match", F.when(F.col("failed_flag_mtom_swab") == 1, 1).otherwise(F.col("failed_match"))
        )  # failed_record
    elif merge_type == "antibody":
        df = df.withColumn(
            "failed_match",
            F.when((F.col("failed_flag_mtom_antibody") == 1) | (F.col("failed_flag_1tom_antibody") == 1), 1).otherwise(
                F.col("failed_match")
            ),
        )  # failed_record

    df = df.withColumn(
        "best_match", F.when(F.col("failed_match") == 1, None).otherwise(F.col("best_match"))
    ).withColumn("not_best_match", F.when(F.col("failed_match") == 1, None).otherwise(F.col("not_best_match")))

    # STEP 2 -
    df_best_match = df.filter(F.col("best_match") == "1")
    df_not_best_match = df.filter(F.col("not_best_match") == "1")
    df_failed_records = df.filter(F.col("failed_match") == "1")

    # STEP 3 -
    df_not_best_match = df_not_best_match.withColumn("not_best_match_for_union", F.lit(1).cast("integer"))

    # out_of_date_range_swab
    df_lab_residuals = df_not_best_match.select(barcode_column_name, *lab_columns_list).distinct()

    # COMMENT: not necessary to drop lab_columns_list if going to be prepared to union with df_best_match
    df_not_best_match = df_not_best_match.drop(*lab_columns_list)
    df_failed_records_iqvia = df_failed_records.drop(*lab_columns_list).distinct()

    # union process: best and not_best match
    df_best_match, df_not_best_match = prepare_for_union(df_best_match, df_not_best_match)
    df_all_iqvia = df_best_match.unionByName(df_not_best_match)

    # union process
    df_all_iqvia, df_failed_records_iqvia = prepare_for_union(df_all_iqvia, df_failed_records_iqvia)
    df_all_iqvia = df_all_iqvia.unionByName(df_failed_records_iqvia)

    # window function count number of unique id
    window = Window.partitionBy("unique_participant_response_id")
    df_all_iqvia = df_all_iqvia.withColumn("unique_id_count", F.count("unique_participant_response_id").over(window))

    df_all_iqvia = df_all_iqvia.filter(
        (F.col("not_best_match_for_union").isNull()) | (F.col("unique_id_count") == 1)
    )  # \
    # .drop('unique_id_count', 'not_best_match')

    # DO: drop all internally created columns

    return (
        df_all_iqvia.drop(*drop_list_columns),
        df_lab_residuals.drop(*drop_list_columns),
        df_failed_records.drop(*drop_list_columns),
    )
