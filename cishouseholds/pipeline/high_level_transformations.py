# flake8: noqa
from typing import List

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_columns_from_array
from cishouseholds.pipeline.timestamp_map import cis_digital_datetime_map


def pivot_vaccine_columns(df: DataFrame, row_number_column: str, prefixes: List[str]):
    """"""
    dfs = []
    original_columns = df.columns
    drop_columns = []
    window = Window.partitionBy(*df.columns).orderBy(F.lit(1))
    for prefix in prefixes:
        cols = [col for col in df.columns if col.startswith(prefix)]
        drop_columns.extend(cols)
        dfs.append(
            df.withColumn(prefix, F.explode(F.array(cols))).withColumn(row_number_column, F.row_number().over(window))
        )

    df = dfs[0]
    for _df in dfs[1:]:
        df = df.join(_df, on=[*original_columns, row_number_column], how="left")
    return df.drop(*drop_columns)


def fill_forwards(df: DataFrame) -> DataFrame:
    # TODO: uncomment for releases after R1
    # df = fill_backwards_overriding_not_nulls(
    #     df=df,
    #     column_identity="participant_id",
    #     ordering_column="visit_date",
    #     dataset_column="survey_response_dataset_major_version",
    #     column_list=fill_forwards_and_then_backwards_list,
    # )

    ## TODO: Not needed until a future release, will leave commented out in code until required
    #
    # df = update_column_if_ref_in_list(
    #     df=df,
    #     column_name_to_update="work_location",
    #     old_value=None,
    #     new_value="Not applicable, not currently working",
    #     reference_column="work_status_v0",
    #     check_list=[
    #         "Furloughed (temporarily not working)",
    #         "Not working (unemployed, retired, long-term sick etc.)",
    #         "Student",
    #     ],
    # )

    return df


def create_formatted_datetime_string_columns(df) -> DataFrame:
    """
    Create columns with specific datetime formatting for use in output data.
    """
    date_format_dict = {
        "visit_date_string": "visit_datetime",
        "samples_taken_date_string": "samples_taken_datetime",
    }
    datetime_format_dict = {
        "visit_datetime_string": "visit_datetime",
        "samples_taken_datetime_string": "samples_taken_datetime",
        "swab_barcode_corrected_datetime_string": "swab_barcode_corrected_datetime",
        "blood_barcode_corrected_datetime_string": "blood_barcode_corrected_datetime",
    }
    date_format_string_list = [
        "date_of_birth",
        "improved_visit_date",
        "think_had_covid_onset_date",
        "think_had_covid_onset_date_raw",
        "cis_covid_vaccine_date",
        "cis_covid_vaccine_date_1",
        "cis_covid_vaccine_date_2",
        "cis_covid_vaccine_date_3",
        "cis_covid_vaccine_date_4",
        "last_suspected_covid_contact_date",
        "last_suspected_covid_contact_date_raw",
        "last_covid_contact_date",
        "last_covid_contact_date_raw",
        "other_covid_infection_test_raw",
        "other_covid_infection_test_first_positive_date",
        "other_antibody_test_last_negative_date",
        "other_antibody_test_first_positive_date",
        "other_covid_infection_test_last_negative_date",
        "been_outside_uk_last_return_date",
        "think_have_covid_onset_date",
        "swab_return_date",
        "swab_return_future_date",
        "blood_return_date",
        "blood_return_future_date",
        "cis_covid_vaccine_date_5",
        "cis_covid_vaccine_date_6",
        "cis_covid_vaccine_date",
        "think_have_covid_symptom_onset_date",  # tempvar
        "other_covid_infection_test_positive_date",  # tempvar
        "other_covid_infection_test_negative_date",  # tempvar
        "other_antibody_test_positive_date",  # tempvar
        "other_antibody_test_negative_date",  # tempvar
        "nhs_share_opt_out_date",
        "contact_known_or_suspected_covid_latest_date",
    ]
    date_format_string_list = [
        col for col in date_format_string_list if col not in cis_digital_datetime_map["yyyy-MM-dd"]
    ] + cis_digital_datetime_map["yyyy-MM-dd"]

    for column_name_to_assign, timestamp_column in date_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for timestamp_column in date_format_string_list:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy",
                lower_case=True,
            )
    for column_name_to_assign, timestamp_column in datetime_format_dict.items():
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=column_name_to_assign,
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    for timestamp_column in cis_digital_datetime_map["yyyy-MM-dd'T'HH:mm:ss'Z'"]:
        if timestamp_column in df.columns:
            df = assign_column_to_date_string(
                df=df,
                column_name_to_assign=timestamp_column + "_string",
                reference_column=timestamp_column,
                time_format="ddMMMyyyy HH:mm:ss",
                lower_case=True,
            )
    return df


def fix_timestamps(df: DataFrame) -> DataFrame:
    """
    Fix any issues with dates saved in timestamp format drifting ahead by n hours.
    """
    date_cols = [c for c in df.columns if "date" in c and "datetime" not in c]
    d_types_list = [list(d) for d in df.select(*date_cols).dtypes]
    d_types = {d[0]: d[1] for d in d_types_list}
    for col in date_cols:
        if d_types[col] == "timestamp":
            df = df.withColumn(col, F.date_format(F.col(col), "yyyy-MM-dd"))
    return df


def get_differences(
    base_df: DataFrame, compare_df: DataFrame, unique_id_column: str, diff_sample_size: int = 10
) -> DataFrame:
    window = Window.partitionBy("column_name").orderBy("column_name")
    cols_to_check = [col for col in base_df.columns if col in compare_df.columns and col != unique_id_column]

    for col in cols_to_check:
        base_df = base_df.withColumnRenamed(col, f"{col}_ref")

    df = base_df.join(compare_df, on=unique_id_column, how="left")

    diffs_df = df.select(
        [
            F.when(F.col(col).eqNullSafe(F.col(f"{col}_ref")), None).otherwise(F.col(unique_id_column)).alias(col)
            for col in cols_to_check
        ]
    )
    diffs_df = diffs_df.select(
        F.explode(
            F.array(
                [
                    F.struct(F.lit(col).alias("column_name"), F.col(col).alias(unique_id_column))
                    for col in diffs_df.columns
                ]
            )
        ).alias("kvs")
    )
    diffs_df = (
        diffs_df.select("kvs.column_name", f"kvs.{unique_id_column}")
        .filter(F.col(unique_id_column).isNotNull())
        .withColumn("ROW", F.row_number().over(window))
        .filter(F.col("ROW") < diff_sample_size)
    ).drop("ROW")

    counts_df = df.select(
        *[
            F.sum(F.when(F.col(c).eqNullSafe(F.col(f"{c}_ref")), 0).otherwise(1)).alias(c).cast("integer")
            for c in cols_to_check
        ],
        *[
            F.sum(F.when((~F.col(c).eqNullSafe(F.col(f"{c}_ref"))) & (F.col(f"{c}_ref").isNotNull()), 1).otherwise(0))
            .alias(f"{c}_non_improved")
            .cast("integer")
            for c in cols_to_check
        ],
    )
    counts_df = counts_df.select(
        F.explode(
            F.array(
                [
                    F.struct(
                        F.lit(col).alias("column_name"),
                        F.col(col).alias("difference_count"),
                        F.col(f"{col}_non_improved").alias("difference_count_non_improved"),
                    )
                    for col in [c for c in counts_df.columns if not c.endswith("_non_improved")]
                ]
            )
        ).alias("kvs")
    )
    counts_df = counts_df.select("kvs.column_name", "kvs.difference_count", "kvs.difference_count_non_improved")
    return counts_df, diffs_df
