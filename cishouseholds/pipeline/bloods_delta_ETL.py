from cishouseholds.derive import assign_column_uniform_value
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
    D1: substring_column
    D11: assign_column_uniform_value

    Parameters
    ----------
    df: pyspark.sql.DataFrame

    Return
    ------
    df: pyspark.sql.DataFrame
    """
    df = substring_column(df, "plate", "Plate Barcode", 5, 5)
    df = assign_column_uniform_value(df, "assay_category", 1)

    return df


def load_bloods_delta():
    pass
