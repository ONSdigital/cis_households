from typing import Dict


class SurveyTableLengths:
    def __init__(self) -> None:
        self.table_lengths: Dict[str, int] = {}

    def set_survey_tables(self, survey_tables):
        self.table_lengths = {table: 0 for table in survey_tables}

    def log_length(self, table_name, length):
        if table_name in self.table_lengths:
            self.table_lengths[table_name] = length

    def check_lengths(self):
        lengths = set(self.table_lengths.values())
        table_lengths_string = "\n".join(
            f"- {table_name}: {table_length}" for table_name, table_length in self.table_lengths.items()
        )
        if len(lengths) != 1:
            raise ValueError(
                f"all survey tables post union should be the same length,\ninstead here are their lengths:\n{table_lengths_string}"
            )
        else:
            print("Success: All survey tables are equal")  # functional


survey_table_lengths = SurveyTableLengths()
