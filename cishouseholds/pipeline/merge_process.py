from pyspark.sql import DataFrame

import cishouseholds.merge as M


def execute_and_resolve_flags_merge(survey_df: DataFrame, labs_df: DataFrame):
    """ """

    survey_df = M.assign_unique_identifier_column(survey_df, "unique_id_voyager", ordering_columns=[])
    labs_df = M.assign_unique_identifier_column(labs_df, "unique_id_swab", ordering_columns=[])

    survey_df = M.assign_count_of_occurrences_column(survey_df, "swab_sample_barcode", "count_barcode_voyager")
    labs_df = M.assign_count_of_occurrences_column(labs_df, "swab_sample_barcode", "count_barcode_swab")

    outer_df = M.join_dataframes(survey_df, labs_df, "swab_sample_cleaned", "outer")

    outer_df = M.assign_time_difference_and_flag_if_outside_interval(
        outer_df,
        "out_of_date_range_swab",
        "diff_vs_visit_hr",
        "visit_datetime",
        "pcr_result_recorded_datetime",
        -24,
        480,
        "hours",
    )

    outer_df = M.assign_absolute_offset(outer_df, "abs_offset_diff_vs_visit_hr", "diff_vs_visit_hr", 24)

    ordering_columns = [
        "abs_offset_diff_vs_visit_hr",
        "diff_vs_visit_hr",
        "visit_date",
        # 4th here is uncleaned barcode from labs
    ]

    outer_df = M.one_to_many_swabs(
        outer_df,
        "out_of_date_range_swab",
        "count_barcode_labs",
        "count_barcode_voyager",
        "swab_sample_barcode",
        ordering_columns,
        "pcr_result_classification",
        "Void",
        "drop_flag_one_to_many_swabs",
    )

    outer_df = M.many_to_one_swab_flag(
        outer_df,
        "drop_flag_many_to_one_swabs",
        "swab_sample_barcode",
        ordering_columns,
    )

    outer_df = M.many_to_many_flag(
        outer_df,
        "drop_flag_many_to_many_swabs",
        "swab_barcode_cleaned",
        ordering_columns,
        "swab",
        "failed_flag_many_to_many_swabs",
    )
