import pyspark.sql.functions as F

from cishouseholds.pipeline.load import extract_from_table
from cishouseholds.pipeline.load import get_run_id


def check_survey_table_lengths():
    tables_df = extract_from_table("table_log").filter(
        (F.col("survey_table") == True) & (F.col("run_id") == get_run_id())  # noqa
    )
    table_names = tables_df.toPandas()["table_name"].to_list()
    lengths = [extract_from_table(table).count() for table in table_names]
    table_lengths_string = "\n".join(
        f"- {table_name}: {table_length}" for table_name, table_length in zip(table_names, lengths)
    )
    if len(set(lengths)) == 0:
        print("No survey tables found")  # functional
    elif len(set(lengths)) > 1:
        table_lengths_error = f"All survey tables post union should be the same length,\ninstead here are their lengths:\n{table_lengths_string}"
        print(table_lengths_error)  # functional
    else:
        print("Success: All survey tables are equal")  # functional
