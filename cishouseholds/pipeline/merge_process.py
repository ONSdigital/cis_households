import cishouseholds.merge as M
from cishouseholds.validate import validate_merge_logic


def execute_and_resolve_flags_merge_specific_swabs(survey_df, labs_df, column_name_date_visit):
    """ """

    outer_df = merge_process_perparation(survey_df, labs_df)

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
    outer_df = merge_process_validation(outer_df)

    return merge_process_filtering(outer_df)


def execute_and_resolve_flags_merge_specific_antibody(survey_df, labs_df, column_name_date_visit):
    """ """
    survey_df = M.assign_unique_identifier_column(survey_df, "unique_id_voyager", ordering_columns=["barcode"])

    survey_df = M.assign_count_of_occurrences_column(survey_df, "barcode", "count_barcode_voyager")

    outer_df = merge_process_perparation(survey_df, labs_df)

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

    outer_df = merge_process_filtering(outer_df, "antibody")

    return merge_process_filtering(outer_df)


def merge_process_perparation(survey_df, labs_df):
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


def merge_process_validation(outer_df, merge_type: str):
    """ """
    # make a FOR loop so that the assign_merge_process_group_flag() does not repeat
    count_barcodes_lab_condition_list = ["==1", ">1", ">1"]
    count_barcodes_voyager_condition_list = [">1", "==1", ">1"]
    if merge_type == "swabs":
        merge_types = ["1_to_ms", "m_to_1s", "m_to_ms"]
        out_of_date_range = "out_of_date_range_swab"
        count_barcode = "count_barcode_swab"
    elif merge_type == "antibody":
        merge_types = ["1_to_ma", "m_to_1a", "m_to_ma"]
        out_of_date_range = "out_of_date_range_antibody"
        count_barcode = "count_barcode_antibody"

    for merge, count_barcodes_lab, count_barcodes_voyager, out_of_date, count in zip(
        merge_types,
        count_barcodes_lab_condition_list,
        count_barcodes_voyager_condition_list,
        out_of_date_range,
        count_barcode,
    ):
        outer_df = M.assign_merge_process_group_flag(
            df=outer_df,
            column_name_to_assign=merge,
            out_of_date_range_flag=out_of_date,
            count_barcode_labs_column_name=count,
            count_barcode_labs_condition=count_barcodes_lab,
            count_barcode_voyager_column_name="count_barcode_voyager",
            count_barcode_voyager_condition=count_barcodes_voyager,
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

    return outer_df


def merge_process_filtering(outer_df):
    # filter output dataframe
    # outer_df = outer_df.filter(F.col())
    # filter not-best match dataframe

    # filter unmatched dataframe
    pass
