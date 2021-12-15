from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import cishouseholds.merge as M
from cishouseholds.compare import prepare_for_union
from cishouseholds.validate import check_singular_match


def merge_process_preparation(
    survey_df: DataFrame,
    labs_df: DataFrame,
    merge_type: str,
    barcode_column_name: str,
    visit_date_column_name: str,
    received_date_column_name: str,
    merge_type_necessary_columns: List[str],
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
    survey_df = M.assign_count_of_occurrences_column(survey_df, barcode_column_name, "count_barcode_voyager")
    labs_df = M.assign_count_of_occurrences_column(labs_df, barcode_column_name, "count_barcode_" + merge_type)

    outer_df = survey_df.join(labs_df, on=barcode_column_name, how="outer")

    if merge_type == "swab":
        interval_upper_bound = 480
        unique_id = "unique_pcr_test_id"

    elif merge_type == "antibody":
        interval_upper_bound = 240
        unique_id = "unique_antibody_test_id"

    # take none:1 and 1:none out
    none_records_logic = (F.col("unique_participant_response_id").isNull()) | (F.col(unique_id).isNull())
    # check if barcode column is null dump the records in none_record_df
    none_record_df = outer_df.filter(none_records_logic)
    outer_df = outer_df.filter(~none_records_logic)
    df = outer_df.select(*merge_type_necessary_columns)

    merge_type_necessary_columns.remove("unique_participant_response_id")
    merge_type_necessary_columns.remove(unique_id)

    df_non_specific_merge = outer_df.drop(*merge_type_necessary_columns).distinct()

    df = M.assign_time_difference_and_flag_if_outside_interval(
        df=df,
        column_name_outside_interval_flag="out_of_date_range_" + merge_type,
        column_name_time_difference="diff_vs_visit_hr_" + merge_type,
        start_datetime_reference_column=visit_date_column_name,
        end_datetime_reference_column=received_date_column_name,
        interval_lower_bound=-24,
        interval_upper_bound=interval_upper_bound,
        interval_bound_format="hours",
    )

    df = M.assign_absolute_offset(
        df=df,
        column_name_to_assign="abs_offset_diff_vs_visit_hr_" + merge_type,
        reference_column="diff_vs_visit_hr_" + merge_type,
        offset=24,
    )

    (
        many_to_many_df,
        one_to_many_df,
        many_to_one_df,
        one_to_one_df,
        no_merge_df,
    ) = assign_merge_process_group_flags_and_filter(df=df, merge_type=merge_type)
    return (
        many_to_many_df,
        one_to_many_df,
        many_to_one_df,
        one_to_one_df,
        no_merge_df,
        df_non_specific_merge,
        none_record_df,
    )


def assign_merge_process_group_flags_and_filter(df: DataFrame, merge_type: str):
    """
    Assign all merge process group flag columns simultaneously and create separate
    into individual dataframes
    Parameters
    ----------
    df
    merge_types
    """
    match_types = {"1to1": ["==1", "==1"], "mtom": [">1", ">1"], "1tom": [">1", "==1"], "mto1": ["==1", ">1"]}
    for name, condition in match_types.items():
        df = M.assign_merge_process_group_flag(
            df=df,
            column_name_to_assign=name + "_" + merge_type,
            out_of_date_range_flag="out_of_date_range_" + merge_type,
            count_barcode_labs_column_name="count_barcode_" + merge_type,
            count_barcode_labs_condition=condition[0],
            count_barcode_voyager_column_name="count_barcode_voyager",
            count_barcode_voyager_condition=condition[1],
        )
    one_to_one_df = df.filter(F.col("1to1" + "_" + merge_type) == 1)

    many_to_many_df = df.filter(F.col("mtom" + "_" + merge_type) == 1)

    one_to_many_df = df.filter(F.col("1tom" + "_" + merge_type) == 1)

    many_to_one_df = df.filter(F.col("mto1" + "_" + merge_type) == 1)

    # TODO: if none_record_df method works, discard no_merge_df
    no_merge_df = df.filter(
        (F.col("mtom" + "_" + merge_type).isNull())
        & (F.col("1tom" + "_" + merge_type).isNull())
        & (F.col("mto1" + "_" + merge_type).isNull())
        & (F.col("1to1" + "_" + merge_type).isNull())
    )
    no_merge_df = no_merge_df.withColumn(
        "noneto1_" + merge_type, F.when((F.col("count_barcode_voyager").isNull()), 1).otherwise(None)
    )
    no_merge_df = no_merge_df.withColumn(
        "1tonone_" + merge_type, F.when((F.col("count_barcode_" + merge_type).isNull()), 1).otherwise(None)
    )

    # validate that only one match type exists at this point

    return many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df, no_merge_df


def merge_process_validation(
    one_to_many_df: DataFrame,
    many_to_one_df: DataFrame,
    many_to_many_df: DataFrame,
    merge_type: str,
    barcode_column_name: str,
) -> DataFrame:
    """
    Once the merging, preparation and one_to_many, many_to_one and many_to_many functions are executed,
    this function will apply the validation function after identifying what type of merge it is.
    Parameters
    ----------
    one_to_many_df
    many_to_one_df
    many_to_many_df
    merge_type
        either swab or antibody, no other value accepted.
    barcode_column_name
    """
    input_dfs = [one_to_many_df, many_to_one_df, many_to_many_df]
    output_dfs = []

    flag_column_names = ["drop_flag_1tom_", "drop_flag_mto1_", "drop_flag_mtom_"]
    flag_column_names_syntax = [column + merge_type for column in flag_column_names]

    failed_column_names = ["failed_1tom_", "failed_mto1_", "failed_mtom_"]
    failed_column_names_syntax = [column + merge_type for column in failed_column_names]

    existing_failure_column_dict = {
        "failed_1tom_antibody": "failed_flag_1tom_antibody",
        "failed_mtom_antibody": "failed_flag_mtom_antibody",
        "failed_mtom_swab": "failed_flag_mtom_swab",
    }

    for df, flag_column, failed_column in zip(input_dfs, flag_column_names_syntax, failed_column_names_syntax):
        existing_failure_column = None
        group_by = [barcode_column_name]

        if failed_column in existing_failure_column_dict:
            existing_failure_column = existing_failure_column_dict[failed_column]

        if "mtom" in failed_column:
            group_by = [barcode_column_name, "unique_participant_response_id"]

        df = check_singular_match(
            df=df,
            drop_flag_column_name=flag_column,
            failure_column_name=failed_column,
            group_by_columns=group_by,
            existing_failure_column=existing_failure_column,
        )
        output_dfs.append(df)
    return tuple(output_dfs)


def execute_merge_specific_swabs(
    survey_df: DataFrame,
    labs_df: DataFrame,
    barcode_column_name: str,
    visit_date_column_name: str,
    received_date_column_name: str,
    void_value: str = "Void",
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
        by default is "Void" but it is possible to specify what is considered as void in the PCR result test.
    """
    merge_type = "swab"
    (
        many_to_many_df,
        one_to_many_df,
        many_to_one_df,
        one_to_one_df,
        no_merge_df,
        df_non_specific_merge,
        none_record_df,
    ) = merge_process_preparation(
        survey_df=survey_df,
        labs_df=labs_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
        visit_date_column_name=visit_date_column_name,
        received_date_column_name=received_date_column_name,
        merge_type_necessary_columns=[
            "swab_sample_barcode",
            "unique_participant_response_id",
            "unique_pcr_test_id",
            "visit_datetime",
            "pcr_result_recorded_datetime",
            "pcr_result_classification",
            "count_barcode_" + merge_type,
            "count_barcode_voyager",
        ],
    )

    window_columns = [
        "abs_offset_diff_vs_visit_hr_swab",
        "diff_vs_visit_hr_swab",
        visit_date_column_name,
        # Stata also uses uncleaned barcode from labs
    ]
    one_to_many_df = M.one_to_many_swabs(
        df=one_to_many_df,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        pcr_result_column_name="pcr_result_classification",
        void_value=void_value,
        flag_column_name="drop_flag_1tom_" + merge_type,
    )
    many_to_one_df = M.many_to_one_swab_flag(
        df=many_to_one_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
    )
    many_to_many_df = M.many_to_many_flag(
        df=many_to_many_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        out_of_date_range_column="out_of_date_range_swab",
        ordering_columns=[
            "abs_offset_diff_vs_visit_hr_swab",
            "diff_vs_visit_hr_swab",
            "unique_participant_response_id",
            "unique_pcr_test_id",
        ],
        process_type="swab",
        failure_column_name="failed_flag_mtom_" + merge_type,
    )
    one_to_many_df, many_to_one_df, many_to_many_df = merge_process_validation(
        one_to_many_df=one_to_many_df,
        many_to_one_df=many_to_one_df,
        many_to_many_df=many_to_many_df,
        merge_type="swab",
        barcode_column_name=barcode_column_name,
    )

    one_to_many_df.cache().count()
    many_to_one_df.cache().count()
    many_to_many_df.cache().count()

    unioned_df = M.union_multiple_tables(
        tables=[many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df, no_merge_df]
    )

    unioned_df = M.null_safe_join(
        unioned_df,
        df_non_specific_merge,
        null_safe_on=["unique_pcr_test_id"],
        null_unsafe_on=["unique_participant_response_id"],
        how="left",
    )
    return unioned_df, none_record_df


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
    (
        many_to_many_df,
        one_to_many_df,
        many_to_one_df,
        one_to_one_df,
        no_merge_df,
        df_non_specific_merge,
        none_record_df,
    ) = merge_process_preparation(
        survey_df=survey_df,
        labs_df=labs_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
        visit_date_column_name=visit_date_column_name,
        received_date_column_name=received_date_column_name,
        merge_type_necessary_columns=[
            "blood_sample_barcode",
            "unique_participant_response_id",
            "unique_antibody_test_id",
            "visit_date_string",
            "blood_sample_received_date_s_protein",
            "antibody_test_result_recorded_date_s_protein",
            "antibody_test_result_classification_s_protein",
            "count_barcode_antibody",
            "count_barcode_voyager",
            "siemens_antibody_test_result_value_s_protein",
        ],
    )

    one_to_many_df = M.one_to_many_antibody_flag(
        df=one_to_many_df,
        column_name_to_assign="drop_flag_1tom_" + merge_type,
        group_by_column=barcode_column_name,
        diff_interval_hours="diff_vs_visit_hr_antibody",
        siemens_column="siemens_antibody_test_result_value_s_protein",
        tdi_column="antibody_test_result_classification_s_protein",
        visit_date=visit_date_column_name,
    )
    many_to_one_df = M.many_to_one_antibody_flag(
        df=many_to_one_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
    )
    window_columns = [
        "abs_offset_diff_vs_visit_hr_antibody",
        "diff_vs_visit_hr_antibody",
        "unique_participant_response_id",
        "unique_antibody_test_id",
    ]
    many_to_many_df = M.many_to_many_flag(
        df=many_to_many_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        process_type=merge_type,
        out_of_date_range_column="out_of_date_range_" + merge_type,
        failure_column_name="failed_flag_mtom_" + merge_type,
    )

    one_to_many_df, many_to_one_df, many_to_many_df = merge_process_validation(
        one_to_many_df=one_to_many_df,
        many_to_one_df=many_to_one_df,
        many_to_many_df=many_to_many_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
    )
    one_to_many_df.cache().count()
    many_to_one_df.cache().count()
    many_to_many_df.cache().count()

    unioned_df = M.union_multiple_tables(
        tables=[many_to_many_df, one_to_many_df, many_to_one_df, one_to_one_df, no_merge_df]
    )

    unioned_df = M.null_safe_join(
        unioned_df,
        df_non_specific_merge,
        null_safe_on=["unique_antibody_test_id"],
        null_unsafe_on=["unique_participant_response_id"],
        how="left",
    )
    return unioned_df, none_record_df


def merge_process_filtering(
    df: DataFrame,
    none_record_df: DataFrame,
    merge_type: str,
    barcode_column_name: str,
    lab_columns_list: List[str],
    merge_combination: List[str] = ["1tom", "mto1", "mtom"],
):
    """
    Resolves all drop/failed flags created in the merging process to filter out
    successfully-matched records from unmatched lab records and failed records.

    Parameters
    ----------
    df
        Input dataframe with drop, merge_type and failed to merge columns
    merge_type
        Must be swab or antibody
    lab_columns_list
        Names of all columns associated only with the lab input
    merge_combination
        Types of merges to resolve, accepted strings in list: 1tom, mto1, mtom

    Notes
    -----
    This function will return 3 dataframes:
    - Successfully matched df inclusive of all iqvia records
    - Residuals df (i.e. all lab records not matched)
    - Failed records df (records that have failed process)
    """
    df = df.withColumn("failed_match", F.lit(None).cast("integer"))

    df = df.withColumn("not_best_match", F.when(F.col(f"out_of_date_range_{merge_type}") == 1, 1))
    df = df.withColumn(
        "best_match",
        F.when(
            (F.col(f"out_of_date_range_{merge_type}").isNull() & (F.col(f"1to1_{merge_type}") == 1)),
            1,
        ),
    )

    for xtox in merge_combination:
        best_match_logic = (
            (F.col(xtox + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + xtox + "_" + merge_type).isNull())
            & (F.col("failed_" + xtox + "_" + merge_type).isNull())
            & (F.col(f"out_of_date_range_{merge_type}").isNull())
        )
        not_best_match_logic = (
            (F.col(xtox + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + xtox + "_" + merge_type) == 1)
            & (F.col("failed_" + xtox + "_" + merge_type).isNull())
        )
        failed_match_logic = (F.col(xtox + "_" + merge_type) == 1) & (F.col("failed_" + xtox + "_" + merge_type) == 1)

        df = df.withColumn("best_match", F.when(best_match_logic, 1).otherwise(F.col("best_match")))
        df = df.withColumn("not_best_match", F.when(not_best_match_logic, 1).otherwise(F.col("not_best_match")))
        df = df.withColumn("failed_match", F.when(failed_match_logic, 1).otherwise(F.col("failed_match")))

    if merge_type == "swab":
        df = df.withColumn(
            "failed_match", F.when(F.col("failed_flag_mtom_swab") == 1, 1).otherwise(F.col("failed_match"))
        )
        lab_record_id = "unique_pcr_test_id"
    elif merge_type == "antibody":
        df = df.withColumn(
            "failed_match",
            F.when((F.col("failed_flag_mtom_antibody") == 1) | (F.col("failed_flag_1tom_antibody") == 1), 1).otherwise(
                F.col("failed_match")
            ),
        )
        lab_record_id = "unique_antibody_test_id"

    df = df.withColumn(
        "best_match", F.when(F.col("failed_match") == 1, None).otherwise(F.col("best_match"))
    ).withColumn("not_best_match", F.when(F.col("failed_match") == 1, None).otherwise(F.col("not_best_match")))

    df_best_match = df.filter(F.col("best_match") == "1")
    df_not_best_match = df.filter(F.col("not_best_match") == "1")
    df_failed_records = df.filter(F.col("failed_match") == "1")

    # Created for filtering purposes later on, to ensure a 'best_match' is chosen over a 'not_best_match' record
    df_not_best_match = df_not_best_match.withColumn("not_best_match_for_union", F.lit(1).cast("integer"))

    df_lab_residuals = df_not_best_match.select(barcode_column_name, *lab_columns_list).distinct()
    df_lab_residuals = df_lab_residuals.join(df_best_match, on=lab_record_id, how="left_anti")

    drop_list_columns = [
        f"out_of_date_range_{merge_type}",
        f"1tom_{merge_type}",
        f"mto1_{merge_type}",
        f"mtom_{merge_type}",
        f"drop_flag_1tom_{merge_type}",
        f"drop_flag_mto1_{merge_type}",
        f"drop_flag_mtom_{merge_type}",
        f"failed_1tom_{merge_type}",
        f"failed_mto1_{merge_type}",
        f"failed_mtom_{merge_type}",
        f"failed_flag_mtom_{merge_type}",
        f"1to1_{merge_type}",
        f"1tonone_{merge_type}",
        f"noneto1_{merge_type}",
        "best_match",
        "not_best_match",
        "failed_match",
    ]
    if merge_type == "antibody":
        drop_list_columns.append("failed_flag_1tom_antibody")

    df_not_best_match = df_not_best_match.drop(*lab_columns_list).drop(*drop_list_columns).distinct()
    df_failed_records_iqvia = df_failed_records.drop(*lab_columns_list).drop(*drop_list_columns).distinct()

    df_best_match, df_not_best_match = prepare_for_union(df_best_match, df_not_best_match)
    df_all_iqvia = df_best_match.unionByName(df_not_best_match)

    df_all_iqvia, df_failed_records_iqvia = prepare_for_union(df_all_iqvia, df_failed_records_iqvia)
    df_all_iqvia = df_all_iqvia.unionByName(df_failed_records_iqvia)

    if merge_type == "swab":
        unique_id = "unique_pcr_test_id"
        drop_list = ["count_barcode_voyager", "count_barcode_swab"]
    elif merge_type == "antibody":
        unique_id = "unique_antibody_test_id"
        drop_list = ["count_barcode_voyager", "count_barcode_antibody"]

    print("        -creating residuals table")  # functional
    df_lab_residuals = M.union_multiple_tables(
        tables=[
            df_lab_residuals,
            none_record_df.filter(F.col("unique_participant_response_id").isNull()).select(*df_lab_residuals.columns),
        ]
    )

    print("        -creating IQVIA only table")  # functional
    df_all_iqvia = M.union_multiple_tables(
        tables=[df_all_iqvia, none_record_df.filter(F.col(unique_id).isNull()).drop(*drop_list)]
    )
    df_all_iqvia = M.union_multiple_tables(tables=[df_all_iqvia, df.where(F.col("1tonone_" + merge_type).isNotNull())])

    # window function count number of unique id
    window = Window.partitionBy("unique_participant_response_id")
    df_all_iqvia = df_all_iqvia.withColumn("unique_id_count", F.count("unique_participant_response_id").over(window))

    df_all_iqvia = df_all_iqvia.filter((F.col("not_best_match_for_union").isNull()) | (F.col("unique_id_count") == 1))

    drop_list_columns.extend(["not_best_match_for_union", "unique_id_count"])
    return (
        df_all_iqvia.drop(*drop_list_columns),
        df_lab_residuals.drop(*drop_list_columns),
        df_failed_records.drop(*drop_list_columns),
    )
