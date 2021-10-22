from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List

import cishouseholds.merge as M
from cishouseholds.validate import validate_merge_logic


def merge_process_perparation(
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
    survey_df,
        iqvia dataframe
    labs_df,
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
    survey_df = M.assign_unique_identifier_column(
        survey_df, "unique_id_voyager", ordering_columns=[barcode_column_name]
    )
    survey_df = M.assign_count_of_occurrences_column(survey_df, barcode_column_name, "count_barcode_voyager")

    labs_df = M.assign_unique_identifier_column(
        labs_df, "unique_id_" + merge_type, ordering_columns=[barcode_column_name]
    )
    labs_df = M.assign_count_of_occurrences_column(labs_df, barcode_column_name, "count_barcode_" + merge_type)

    outer_df = M.join_dataframes(
        survey_df, labs_df, barcode_column_name, "outer"
    )  # refactoring might be needed IF barcode column names in survey_df/labs_df is different

    outer_df = M.assign_time_difference_and_flag_if_outside_interval(
        df=outer_df,
        column_name_outside_interval_flag="out_of_date_range_" + merge_type,
        column_name_time_difference="diff_vs_visit_hr",
        start_datetime_reference_column=visit_date_column_name,
        end_datetime_reference_column=received_date_column_name,
        interval_lower_bound=-24,
        interval_upper_bound=480,
        interval_bound_format="hours",
    )
    outer_df = M.assign_absolute_offset(
        df=outer_df, column_name_to_assign="abs_offset_diff_vs_visit_hr", reference_column="diff_vs_visit_hr", offset=24
    )
    return outer_df


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
        match_type_colums=match_type_colums_syntax,
        group_by_column=barcode_column_name,
    )
    return outer_df


def execute_and_resolve_flags_merge_specific_swabs(
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
    outer_df = merge_process_perparation(
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
        # 4th here is uncleaned barcode from labs
    ]
    outer_df = M.one_to_many_swabs(
        df=outer_df,
        out_of_date_range_flag="out_of_date_range_" + merge_type,
        count_barcode_labs_column_name="count_barcode_" + merge_type,
        count_barcode_voyager_column_name="count_barcode_voyager",
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        pcr_result_column_name="pcr_result_classification",
        void_value=void_value,
        flag_column_name="drop_flag_1tom_" + merge_type,
    )
    outer_df = M.many_to_one_swab_flag(
        df=outer_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
    )
    outer_df = M.many_to_many_flag(
        df=outer_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        process_type="swab",
        failed_flag_column_name_to_assign="failed_flag_mtom_" + merge_type,
    )
    outer_df = merge_process_validation(
        outer_df=outer_df,
        merge_type="swab",
        barcode_column_name=barcode_column_name,
    )
    return outer_df


def execute_and_resolve_flags_merge_specific_antibody(
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
    outer_df = merge_process_perparation(
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
        # 4th here is uncleaned barcode from labs - CHECK
    ]
    outer_df = M.one_to_many_antibody_flag(  # CHECK: dropping records
        df=outer_df,
        column_name_to_assign="drop_flag_1tom_" + merge_type,
        group_by_column=barcode_column_name,
        diff_interval_hours="diff_vs_visit_hr",
        siemens_column="siemens",
        tdi_column="tdi",
        visit_date=visit_date_column_name,
        out_of_date_range_column="out_of_date_range_" + merge_type,
        count_barcode_voyager_column_name="count_barcode_voyager",
        count_barcode_labs_column_name="count_barcode_" + merge_type,
    )
    outer_df = M.many_to_one_antibody_flag(
        df=outer_df,
        column_name_to_assign="drop_flag_mto1_" + merge_type,
        group_by_column=barcode_column_name,
    )
    outer_df = M.many_to_many_flag(
        df=outer_df,
        drop_flag_column_name_to_assign="drop_flag_mtom_" + merge_type,
        group_by_column=barcode_column_name,
        ordering_columns=window_columns,
        process_type=merge_type,
        failed_flag_column_name_to_assign="failed_flag_mtom_" + merge_type,
    )
    outer_df = merge_process_validation(
        outer_df,
        merge_type=merge_type,
        barcode_column_name=barcode_column_name,
    )
    outer_df.toPandas().to_csv("output.csv", index=False)
    return outer_df


def merge_process_filtering(
    df: DataFrame,
    merge_type: str,
    merge_combination: List[str] = ["1tom", "mto1", "mtom"],
    drop_list_columns: List[str] = [],
) -> DataFrame:
    """
    Final filtering process of merging generating sucessful merges ...
    Parameters
    ----------
    df
        input dataframe with drop, merge_type and failed to merge columns
    merge_type
        either swab or antibody, anything else will fail.
    merge_combination
        only elements in the list accepted 1tom, mto1, mtom
    drop_list_columns
        present in a list the exact name of columns to be dropped for the final
        3 dataframes df_best_match, df_not_best_match, df_failed_records
    Notes: this function will return 2 dataframes, one with best match records
    another one with not best matched records
    """
    for element in merge_combination:
        df_best_match = df.filter(
            (F.col(element + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + element + "_" + merge_type).isNull())
            & (F.col("failed_" + element + "_" + merge_type).isNull())
        ).drop(*drop_list_columns)

        df_not_best_match = df.filter(
            (F.col(element + "_" + merge_type) == 1)
            & (F.col("drop_flag_" + element + "_" + merge_type) == 1)
            & (F.col("failed_" + element + "_" + merge_type).isNull())
        ).drop(*drop_list_columns)

        if merge_type == "swab":  # only happens when antibody match already ocurred
            df_failed_records = df.filter(
                (F.col(element + "_" + merge_type) == 1) & (F.col("failed_" + element + "_" + merge_type) == 1)
            ).drop(*drop_list_columns)

    if merge_type == "swab":
        return df_best_match, df_not_best_match, df_failed_records
    else:
        return df_best_match, df_not_best_match
