import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.pipeline.merge_process import execute_merge_specific_swabs

# from cishouseholds.pipeline.merge_process import merge_process_filtering
# from cishouseholds.pipeline.merge_process import merge_process_preparation
# from cishouseholds.pipeline.merge_process import merge_process_validation


def test_merge_process_swab(spark_session):
    schema = "barcode string, comments_surv string"
    data = [
        ("A", "1to1"),
        ("B", "mto1"),
        ("C", "1tom"),
        ("C", "1tom"),
        ("C", "1tom"),
        ("D", "mtom"),
        ("D", "mtom"),
        ("D", "mtom"),
        ("E", "1to1"),
        ("F", "mto1"),
        ("G", "1tom"),
        ("G", "1tom"),
        ("H", "mtom"),
        ("H", "mtom"),
        ("J", "1to1"),
        ("K", "mto1"),
        ("L", "1tom"),
        ("L", "1tom"),
        ("M", "mtom"),
        ("M", "mtom"),
        ("N", "mto1"),
        ("P", "1tom"),
        ("P", "1tom"),
        ("Q", "mtom"),
        ("Q", "mtom"),
        ("R", "1to1"),
        ("S", "1to1"),
        ("T", "1to1"),
    ]
    df_input_survey = spark_session.createDataFrame(data, schema=schema)

    schema = """
                barcode string,
                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                comments_lab string
            """
    data = [
        # fmt: off
        ("A", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type 1to1"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mto1"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mto1"),
        ("B", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mto1"),
        ("C", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type 1tom"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mtom"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mtom"),
        ("D", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "merge_type mtom"),
        ("E", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "1to1 out of range"),
        ("F", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "1tom out of range"),
        ("F", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "1tom out of range"),
        ("G", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "mto1 out of range"),
        ("H", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "mtom out of range"),
        ("H", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "mtom out of range"),
        ("J", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "1to1 NOT out of range"),
        ("K", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "1tom NOT out of range"),
        ("K", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "1tom NOT out of range"),
        ("L", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "mto1 NOT out of range"),
        ("M", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "mtom NOT out of range"),
        ("M", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "mtom NOT out of range"),
        ("N", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "1tom time_order_flag - closest to visit"),
        ("N","2020-01-01","2020-01-03","2020-01-04 12:00:00","positive","1tom time_order_flag - NOT closest to visit"),
        ("P","2020-01-01","2020-01-04","2020-01-04 12:00:00","positive","mto1 time_order_flag - NOT closest to visit"),
        ("Q", "2020-01-01", "2020-01-02", "2020-01-04 12:00:00", "positive", "mtom time_order_flag - closest to visit"),
        ("Q","2020-01-01","2020-01-03","2020-01-04 12:00:00","positive","mtom time_order_flag - NOT closest to visit"),
        ("R", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "positive", "1to1 pcr_flag"),
        ("S", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "negative", "1to1 pcr_flag"),
        ("T", "2020-01-01", "2020-06-02", "2020-01-04 12:00:00", "void", "1to1 pcr_flag"),
        # fmt:on
    ]
    df_input_labs = spark_session.createDataFrame(data, schema=schema)

    schema = """
                barcode string,
                unique_participant_response_id integer,
                count_barcode_voyager integer,
                date_visit string,
                date_received string,
                pcr_result_recorded_datetime string,
                pcr_result_classification string,
                unique_pcr_test_id integer,
                count_barcode_swab integer,
                diff_vs_visit_hr float,
                out_of_date_range_swab integer,
                abs_offset_diff_vs_visit_hr float,
                identify_1tom_swabs_flag integer,
                time_order_flag integer,
                pcr_flag integer,
                time_difference_flag integer,
                drop_flag_1tom_swab integer,
                identify_mto1_swab_flag integer,
                drop_flag_mto1_swab integer,
                identify_mtom_flag integer,
                failed_flag_mtom_swab integer,
                drop_flag_mtom_swab integer,
                1tom_swab integer,
                mto1_swab integer,
                mtom_swab integer,
                failed_1tom_swab integer,
                failed_mto1_swab integer,
                failed_mtom_swab integer,
                best_match integer,
                not_best_match integer,
                failed_match integer
            """

    data = [
        # fmt: off
        ("A",1,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",1,1,24.0,None,0.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None,None),
        ("B",2,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",4,3,24.0,None,0.0,1,None,None,None,1,None,None,None,None,None,None,1,None,None,1,None,None,None,1),
        ("B",2,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",3,3,24.0,None,0.0,1,None,None,None,1,None,None,None,None,1,None,1,None,None,1,None,None,None,1),
        ("B",2,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",2,3,24.0,None,0.0,1,None,None,None,1,None,None,None,None,1,None,1,None,None,1,None,None,None,1),
        ("C",5,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",5,1,24.0,None,0.0,None,None,None,None,None,1,1,None,None,None,1,None,None,None,None,None,1,None,None),
        ("C",4,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",5,1,24.0,None,0.0,None,None,None,None,None,1,1,None,None,1,1,None,None,None,None,None,1,None,None),
        ("C",3,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",5,1,24.0,None,0.0,None,None,None,None,None,1,1,None,None,1,1,None,None,None,None,None,1,None,None),
        ("D",6,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",6,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("D",7,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",7,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("D",8,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",8,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("D",7,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",6,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("D",6,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",7,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("D",8,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",7,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("D",8,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",6,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("D",7,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",8,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("D",6,3,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",8,3,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("E",9,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",9,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("F",10,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",11,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("F",10,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",10,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,1,None,None,None,None,None,None,None,1,None),
        ("G",12,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",12,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("G",11,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",12,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,1,None,None,None,None,None,None,None,1,None),
        ("H",14,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",14,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("H",13,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",13,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("H",14,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",13,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,1,None,None,None,None,None,None,None,1,None),
        ("H",13,2,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",14,2,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,1,None,None,None,None,None,None,None,1,None),
        ("J",15,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",15,1,24.0,None,0.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None,None),
        ("K",16,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",17,2,24.0,None,0.0,1,None,None,None,1,None,None,None,None,None,None,1,None,None,1,None,None,None,1),
        ("K",16,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",16,2,24.0,None,0.0,1,None,None,None,1,None,None,None,None,1,None,1,None,None,1,None,None,None,1),
        ("L",18,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",18,1,24.0,None,0.0,None,None,None,None,None,1,1,None,None,None,1,None,None,None,None,None,1,None,None),
        ("L",17,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",18,1,24.0,None,0.0,None,None,None,None,None,1,1,None,None,1,1,None,None,None,None,None,1,None,None),
        ("M",19,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",19,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("M",20,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",20,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("M",20,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",19,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("M",19,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",20,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("N",21,1,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",21,2,24.0,None,0.0,1,None,None,None,1,None,None,None,None,None,None,1,None,None,1,None,None,None,1),
        ("N",21,1,"2020-01-01","2020-01-03","2020-01-04 12:00:00","positive",22,2,48.0,None,24.0,1,1,None,1,1,None,None,None,None,1,None,1,None,None,1,None,None,None,1),
        ("P",23,2,"2020-01-01","2020-01-04","2020-01-04 12:00:00","positive",23,1,72.0,None,48.0,None,None,None,None,None,1,1,None,None,None,1,None,None,None,None,None,1,None,None),
        ("P",22,2,"2020-01-01","2020-01-04","2020-01-04 12:00:00","positive",23,1,72.0,None,48.0,None,None,None,None,None,1,1,None,None,1,1,None,None,None,None,None,1,None,None),
        ("Q",25,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",24,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("Q",24,2,"2020-01-01","2020-01-03","2020-01-04 12:00:00","positive",25,2,48.0,None,24.0,None,1,None,1,1,None,None,1,None,None,None,None,1,None,None,1,None,None,1),
        ("Q",24,2,"2020-01-01","2020-01-02","2020-01-04 12:00:00","positive",24,2,24.0,None,0.0,None,None,None,None,None,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("Q",25,2,"2020-01-01","2020-01-03","2020-01-04 12:00:00","positive",25,2,48.0,None,24.0,None,1,None,1,1,None,None,1,None,1,None,None,1,None,None,1,None,None,None),
        ("R",26,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","positive",26,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("S",27,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","negative",27,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        ("T",28,1,"2020-01-01","2020-06-02","2020-01-04 12:00:00","    void",28,1,3672.0,1,3648.0,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,1,None),
        # fmt: on
    ]
    expected_df = spark_session.createDataFrame(data, schema=schema)

    df = execute_merge_specific_swabs(
        survey_df=df_input_survey,
        labs_df=df_input_labs,
        barcode_column_name="barcode",
        visit_date_column_name="date_visit",
        received_date_column_name="date_received",
    )

    assert_df_equality(expected_df.drop("comments"), df, ignore_column_order=True, ignore_row_order=True)

    # CHECK A: no more than one combination type possible = all rows must be smaller than 1
    df = (
        df.withColumn("temp1", F.when(F.col("1tom_swab").isNull(), 0).otherwise(F.col("1tom_swab")))
        .withColumn("temp2", F.when(F.col("mto1_swab").isNull(), 0).otherwise(F.col("mto1_swab")))
        .withColumn("temp3", F.when(F.col("mtom_swab").isNull(), 0).otherwise(F.col("mtom_swab")))
        .withColumn("only_one_comb", F.col("temp1") + F.col("temp2") + F.col("temp3"))
        .withColumn("only_one_comb", F.when(F.col("only_one_comb") > 1, "fail").otherwise("pass"))
        .drop("temp1", "temp2", "temp3")
    )

    # CHECK B: either best_match, not_best_match or failed_match = all rows must be 1
    df = (
        df.withColumn("temp1", F.when(F.col("best_match").isNull(), 0).otherwise(F.col("best_match")))
        .withColumn("temp2", F.when(F.col("not_best_match").isNull(), 0).otherwise(F.col("not_best_match")))
        .withColumn("temp3", F.when(F.col("failed_match").isNull(), 0).otherwise(F.col("failed_match")))
        .withColumn("only_one_type", F.col("temp1") + F.col("temp2") + F.col("temp3"))
        .withColumn("only_one_type", F.when(F.col("only_one_type") != 1, "fail").otherwise("pass"))
        .drop("temp1", "temp2", "temp3")
    )

    # CHECK C: Theres only one best match = all rows must be no larger than 1
    best_match_window = Window.partitionBy("barcode", "best_match")
    df = df.withColumn("only_bestmatch", F.count(F.col("best_match")).over(best_match_window)).withColumn(
        "only_bestmatch", F.when(F.col("only_bestmatch") > 1, "fail").otherwise("pass")
    )

    # CHECK D: no records filtered out before splitting dataframes - number of distinct records
    df_unique_voyager = df.dropDuplicates(["barcode", "unique_participant_response_id"])
    df_unique_swab = df.dropDuplicates(["barcode", "unique_pcr_test_id"])

    assert_df_equality(
        df_unique_voyager.select("barcode"),
        df_input_survey.select("barcode"),
        ignore_column_order=True,
        ignore_row_order=True,
    )

    assert_df_equality(
        df_unique_swab.select("barcode"),
        df_input_labs.select("barcode"),
        ignore_column_order=True,
        ignore_row_order=True,
    )

    # voyager
    # df_records = (
    #     df_all_iqvia.select("barcode", "unique_participant_response_id")
    #     .union(df_failed_records.select("barcode", "unique_participant_response_id"))
    #     .distinct()
    # )

    # assert_df_equality(
    #     df_records.select("barcode"), df_input_labs.select("barcode"), ignore_column_order=True, ignore_row_order=True
    # )

    # # labs: df_lab_residuals should have its unique id
    # df_records = (
    #     df_all_iqvia.select("barcode", "unique_pcr_test_id")
    #     .union(df_lab_residuals.select("barcode", "unique_pcr_test_id"))
    #     .distinct()
    # )

    # assert_df_equality(
    #     df_records.select("barcode"), df_input_labs.select("barcode"), ignore_column_order=True, ignore_row_order=True
    # )
