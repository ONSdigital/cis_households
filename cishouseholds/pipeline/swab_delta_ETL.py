from pyspark.sql import DataFrame

from cishouseholds.derive import assign_column_to_date_string
from cishouseholds.derive import assign_filename_column
from cishouseholds.derive import assign_isin_list
from cishouseholds.derive import assign_unique_id_column
from cishouseholds.derive import derive_cq_pattern
from cishouseholds.derive import mean_across_columns
from cishouseholds.edit import clean_barcode
from cishouseholds.pyspark_utils import get_or_create_spark_session


def transform_swab_delta(df: DataFrame) -> DataFrame:
    """
    Transform swab delta - derive new fields that do not depend on merging with survey responses.
    """
    spark_session = get_or_create_spark_session()
    df = assign_filename_column(df, "swab_test_source_file")
    df = clean_barcode(df=df, barcode_column="swab_sample_barcode")
    df = assign_column_to_date_string(df, "pcr_result_recorded_date_string", "pcr_result_recorded_datetime")
    df = derive_cq_pattern(
        df, ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"], spark_session
    )
    df = assign_unique_id_column(
        df, "unique_pcr_test_id", ["swab_sample_barcode", "pcr_result_recorded_datetime", "cq_pattern"]
    )

    df = mean_across_columns(
        df, "mean_pcr_cq_value", ["orf1ab_gene_pcr_cq_value", "n_gene_pcr_cq_value", "s_gene_pcr_cq_value"]
    )
    df = assign_isin_list(
        df=df,
        column_name_to_assign="one_positive_pcr_target_only",
        reference_column="cq_pattern",
        values_list=["N only", "OR only", "S only"],
        true_false_values=[1, 0],
    )
    return df
