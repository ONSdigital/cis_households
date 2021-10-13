import cishouseholds.merge as M
from cishouseholds.validate import validate_merge_logic


def execute_and_resolve_flags_merge_specific_swabs(survey_df, labs_df, column_name_date_visit):
    """ """

    outer_df = execute_and_resolve_flags_merge_part1(survey_df, labs_df)

    window_columns = [
        "abs_offset_diff_vs_visit_hr",
        "diff_vs_visit_hr",
        column_name_date_visit,  # ="date_visit"
        # 4th here is uncleaned barcode from labs
    ]

    outer_df = M.one_to_many_swabs(
        df=outer_df,
        out_of_date_range_flag="out_of_date_range_swab",
        count_barcode_labs_column_name="count_barcode_swab",
        count_barcode_voyager_column_name="count_barcode_voyager",
        group_by_column="barcode",
        ordering_columns=window_columns,
        pcr_result_column_name="pcr_result_classification",
        void_value="Void",
        flag_column_name="drop_flag_one_to_many_swabs",
    )

    outer_df = M.many_to_one_swab_flag(
        df=outer_df,
        column_name_to_assign="drop_flag_many_to_one_swabs",
        group_by_column="barcode",
        ordering_columns=window_columns,
    )

    outer_df = M.many_to_many_flag(
        df=outer_df,
        drop_flag_column_name_to_assign="drop_flag_many_to_many_swabs",
        group_by_column="barcode",
        ordering_columns=window_columns,
        process_type="swab",
        failed_flag_column_name_to_assign="failed_flag_many_to_many_swabs",
    )
    outer_df = execute_and_resolve_flags_merge_part2(outer_df)

    return outer_df


def execute_and_resolve_flags_merge_specific_antibody(survey_df, labs_df, column_name_date_visit):
    """ """
    outer_df = execute_and_resolve_flags_merge_part1(survey_df, labs_df)

    window_columns = [
        "abs_offset_diff_vs_visit_hr",
        "diff_vs_visit_hr",
        column_name_date_visit,  # ="date_visit"
        # 4th here is uncleaned barcode from labs
    ]
    outer_df = M.one_to_many_antibody_flag(  # CHECK: should it be called antibody
        df=outer_df,
        column_name_to_assign="drop_flag_one_to_many_antibody",
        group_by_column="barcode",
        diff_interval_hours="diff_interval_hours",
        siemens_column="siemens",
        tdi_column="tdi",
        visit_date="date_visit",
        out_of_date_range_column="out_of_date_range_antibody",
        count_barcode_voyager_column_name="count_barcode_voyager",
        count_barcode_labs_column_name="count_barcode_antibody",
    )
    outer_df = M.many_to_one_antibody_flag(
        df=outer_df,
        column_name_to_assign="drop_flag_many_to_one_antibody",
        group_by_column="barcode",
    )
    outer_df = M.many_to_many_flag(
        df=outer_df,
        drop_flag_column_name_to_assign="drop_flag_many_to_many_antibody",
        group_by_column="barcode",
        ordering_columns=window_columns,
        process_type="antibody",
        failed_flag_column_name_to_assign="failed_flag_many_to_many_antibody",
    )

    outer_df = execute_and_resolve_flags_merge_part2(outer_df)

    return outer_df


def execute_and_resolve_flags_merge_part1(survey_df, labs_df):
    """ """
    labs_df = M.assign_unique_identifier_column(labs_df, "unique_id_swab", ordering_columns=["barcode"])
    labs_df = M.assign_count_of_occurrences_column(labs_df, "barcode", "count_barcode_swab")

    outer_df = M.join_dataframes(survey_df, labs_df, "barcode", "outer")

    outer_df = M.assign_time_difference_and_flag_if_outside_interval(
        df=outer_df,
        column_name_outside_interval_flag="out_of_date_range_swab",
        column_name_time_difference="diff_vs_visit_hr",
        start_datetime_reference_column="date_visit",
        end_datetime_reference_column="date_received",
        interval_lower_bound=-24,
        interval_upper_bound=480,
        interval_bound_format="hours",
    )
    outer_df = M.assign_absolute_offset(outer_df, "abs_offset_diff_vs_visit_hr", "diff_vs_visit_hr", 24)

    return outer_df


def execute_and_resolve_flags_merge_part2(outer_df):
    """ """
    # make a FOR loop so that the assign_merge_process_group_flag() does not repeat
    outer_df = M.assign_merge_process_group_flag(
        outer_df,
        "1_to_ms",
        "out_of_date_range_swab",
        "count_barcode_swab",
        "==1",
        "count_barcode_voyager",
        ">1",
    )
    outer_df = M.assign_merge_process_group_flag(
        outer_df,
        "m_to_1s",
        "out_of_date_range_swab",
        "count_barcode_swab",
        ">1",
        "count_barcode_voyager",
        "==1",
    )
    outer_df = M.assign_merge_process_group_flag(
        outer_df,
        "m_to_ms",
        "out_of_date_range_swab",
        "count_barcode_swab",
        ">1",
        "count_barcode_voyager",
        ">1",
    )
    outer_df = validate_merge_logic(
        df=outer_df,
        flag_column_names=[
            "drop_flag_one_to_many_swabs",
            "drop_flag_many_to_one_swabs",
            "drop_flag_many_to_many_swabs",
        ],
        failed_column_names=["failed_one_to_many_swabs", "failed_many_to_one_swabs", "failed_many_to_many_swabs"],
        match_type_colums=["1_to_ms", "m_to_1s", "m_to_ms"],
        group_by_column="barcode",
    )
    # filter output dataframe

    # filter not-best match dataframe

    # filter unmatched dataframe

    return outer_df
