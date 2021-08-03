from cishouseholds.derive import assign_column_uniform_value
from cishouseholds.derive import create_column_from_coalesce
from cishouseholds.derive import substring_column


def bloods_delta_ETL():
    extract_bloods_delta()
    transform_bloods_delta()
    load_bloods_delta()


def extract_bloods_delta():
    pass


def transform_bloods_delta(df):
    """
    Call functions to process input for bloods deltas.
    D1: substring_column(new_column_name: 'plate', column_to_substr: 'plate_tdi', start_position: 5, len_of_substr: 10)
    D11: assign_column_uniform_value(column_name_to_assign: 'assay_category', uniform_value: 1):
    D6: create_column_from_coalesce(new_column_name: 'result_combined', *args: ['result_tdi', 'result_siemens'])

    Parameters
    ----------
    df: pyspark.sql.DataFrame

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    df = substring_column(df, "plate", "plate_tdi", 5, 5)  # Is that 5-10 or 4-9 if 1st char is 0?
    df = assign_column_uniform_value(df, "assay_category", 1)
    df = create_column_from_coalesce(df, "result_combined", "result_tdi", "result_siemens")

    return df


def load_bloods_delta():
    pass
