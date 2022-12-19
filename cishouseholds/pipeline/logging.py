from typing import Dict


class SurveyTableLengths:

    table_lengths: Dict[str, int] = {}

    @classmethod
    def set_survey_tables(cls, survey_tables):
        cls.table_lengths = {table: 0 for table in survey_tables}

    @classmethod
    def log_length(cls, table_name, length):
        if table_name in cls.table_lengths:
            cls.table_lengths[table_name] = length

    @classmethod
    def check_lengths(cls):
        lengths = set(cls.table_lengths.values())
        table_lengths_string = "\n".join(
            f"- {table_name}: {table_length}" for table_name, table_length in cls.table_lengths.items()
        )
        if len(lengths) != 1:
            raise ValueError(
                f"all survey tables post union should be the same length,\ninstead here are their lengths:\n{table_lengths_string}"
            )
        else:
            print("Success: All survey tables are equal")  # functional
