from ..derive import assign_column_convert_to_date
from ..derive import derive_ctpattern

# from ..derive import assign_from_lookup
# from ..derive import mean_across_columns


def swabs_delta_ETL():
    extract_swabs_delta()
    transform_swabs_delta()
    load_swabs_delta()


def extract_swabs_delta():
    pass


def transform_swabs_delta(df, spark_session):
    """
    13: assign_column_convert_to_date
        derived variable name: result_mk_date_time
        V1-Vn: result_mk_date_time
    7: derive_ctpattern
        derived variable name: ctpattern
        V1-Vn: ctORF1ab_result,ctNgene_result,ctSgene_result,ctMS2_result
    9: mean_across_columns
        dvn: ct_mean
        V1-Vn: ctpattern,ctORF1ab,ctNgene,ctSgene
    10: assign_from_lookup
        dvn: ctonetarget
        V1-Vn: ctpattern
    """

    df = assign_column_convert_to_date(df, "result_mk_date_time", "result_mk_date_time")
    df = derive_ctpattern(df, ["ctORF1ab_result", "ctNgene_result", "ctSgene_result"], spark_session)


def load_swabs_delta():
    pass
