from pyspark.sql import SparkSession

from cishouseholds.merge import one_to_many_bloods_flag


spark = SparkSession.builder.getOrCreate()

# if True: #def test_flag_out_of_date_range(spark_session):

schema_iq = """count_iqvia integer, visit_date string, barcode_iq string"""
data_iq = [
    (1, "2029-01-01", "ONS00000001"),
    (1, "2029-01-01", "ONS00000002"),
    (1, "2029-01-01", "ONS00000003"),
    (1, "2029-01-02", "ONS00000003"),
    (2, "2029-01-01", "ONS00000004"),
    (2, "2029-01-02", "ONS00000004"),
    (1, "2029-01-04", "ONS00000003"),
    (1, "2029-01-01", "ONS00000003"),
    (1, "2029-01-02", "ONS00000004"),
    (1, "2029-01-04", "ONS00000004"),
]
schema_ox = """barcode_mk string, received_ox_date string, count_blood integer"""
data_ox = [
    ("ONS00000001", "2029-01-02", 1),
    ("ONS00000002", "2029-01-02", 2),
    ("ONS00000002", "2029-01-04", 2),
    ("ONS00000003", "2029-01-01", 1),
    ("ONS00000004", "2029-01-02", 2),
    ("ONS00000004", "2029-01-05", 2),
    ("ONS00000002", "2029-01-04", 2),
    ("ONS00000003", "2029-01-01", 4),
    ("ONS00000004", "2029-01-02", 2),
    ("ONS00000004", "2029-01-05", 3),
]
df_iq = spark.createDataFrame(data_iq, schema=schema_iq)
df_ox = spark.createDataFrame(data_ox, schema=schema_ox)

df_iq.show()
df_ox.show()
# IQ - 1:m - bloods

df_mrg = df_iq.join(df_ox, df_iq.barcode_iq == df_ox.barcode_mk, "inner")
df_mrg.show()
# df_mrg = assign_date_interval_and_flag(
#    df_mrg, "outside_interval_flag", "diff_interval", "visit_date", "received_ox_date", -24, 48
# )
# df_mrg.show()
# df_mrg.withColumn('grouping_window', F.first()).show()
# new_df = merge_one_to_many_iqvia_blood(df_mrg)
new_df = one_to_many_bloods_flag(df_mrg, "iq_1_to_m_bloods", "barcode_iq")
new_df.show()
