import csv
import inspect
import re
from collections import Counter
from io import StringIO
from pathlib import Path
from typing import Dict
from typing import List
from typing import Union

from pyspark import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from cishouseholds.extract import list_contents
from cishouseholds.pipeline.load import add_error_file_log_entry
from cishouseholds.pyspark_utils import column_to_distinct_list
from cishouseholds.pyspark_utils import get_or_create_spark_session


def validate_csv_fields(text_file: RDD, delimiter: str = ","):
    """
    Function to validate the number of fields within records of a csv file.

    Parameters
    ----------
    text_file : RDD
        A text file (csv) that has been ready by spark context
    delimiter : str
        Delimiter used in csv file, default as ','

    Returns
    -------
    bool
        True if all rows have the expected number of columns or False
        if any rows exist with a different number of fields.
    """

    def count_fields_in_row(delimiter, row):
        """
        Return the number of fields for a given row, as separated by the given delimiter.
        """
        f = StringIO(row)
        reader = csv.reader(f, delimiter=delimiter)
        n_fields = len(next(reader))
        return n_fields

    def check_field(delimiter, row, number_of_columns):
        """
        Check whether number of fields on a given row matches expected number of columns.
        Return True boolean if numbers match and return False if not the case.
        """
        if len(row) > 2 and count_fields_in_row(delimiter, row) != number_of_columns:
            return True
        return False

    header = text_file.first()
    number_of_columns = count_fields_in_row(delimiter, header)
    error_rows = text_file.filter(lambda row: check_field(delimiter, row, number_of_columns)).collect()
    row_counts = text_file.map(lambda row: count_fields_in_row(delimiter, row)).collect()
    print("Row counts: ", Counter(row_counts))  # functional
    print(
        f"There were {len(error_rows)} erroneous rows out of {text_file.count()} total rows: {error_rows}"
    )  # functional
    return True if len(error_rows) == 0 else False


def normalise_schema(file_path: str, reference_validation_schema: dict, regex_schema: dict):
    """
    Use a series of regex patterns mapped to correct column names to build an individual schema
    for a given csv input file that has varied headings across a group of similar files.

    Parameters
    ----------
    file_path: str
        File path to input data file.
    reference_validation_schema: dict
        ???
    regex_schema: dict
        ???

    Returns
    -------
    str or DataFrame
        If validation check failed and unrcognised columns are found, returns custom error message. Otherwise,
        returns dataframe of ???
    """
    spark_session = get_or_create_spark_session()

    file = spark_session.sparkContext.textFile(file_path)
    found = False
    count = 0
    used = []
    while not found and count < 100:
        first = file.first()
        header = next(csv.reader(StringIO(first), delimiter=","))
        used.append(first)
        file = file.filter(lambda line: line not in used)
        if len([x for x in header if x != ""]) >= len(reference_validation_schema.keys()):
            found = True
        count += 1

    actual_header = header
    validation_schema = []
    dont_drop_list = []
    if actual_header != list(reference_validation_schema.keys()):
        for actual_col in actual_header:
            actual_col = "DROP" if actual_col == "" else actual_col.replace(" ", "_")
            actual_col = re.sub(r"[^a-zA-Z0-9_]", "", actual_col)
            matched = False
            for regex, normalised_column in regex_schema.items():
                if re.search(rf"{regex}", actual_col):
                    validation_schema.append(
                        [normalised_column, reference_validation_schema[normalised_column]["type"]]
                    )
                    dont_drop_list.append(actual_col)
                    matched = True
                    break
            if not matched:
                validation_schema.append([actual_col, "string"])
        if not all(
            [col in [col_name[0] for col_name in validation_schema] for col in reference_validation_schema.keys()]
        ):
            error_message = (
                f"{file_path} is invalid as header({actual_header} contained unrecognisable columns"  # functional
            )
            return error_message, None
        drop = [
            *[
                "".join(filter(lambda x: x not in r"./\|", col.replace(" ", "_")))
                for col in actual_header
                if col not in dont_drop_list
            ],
            "DROP",
        ]
    else:
        validation_schema = [[col, _type] for col, _type in reference_validation_schema.items()]
        drop = []
    df = (
        file.mapPartitions(lambda line: csv.reader(line, delimiter=",", quotechar='"'))
        .toDF(",".join([f"{col[0]} {col[1]}" for col in validation_schema]))
        .drop(*drop)
    )
    return None, df


def validate_csv_header(text_file: RDD, expected_header: List[str], delimiter: str = ","):
    """
    Function to validate header in csv file matches expected header.

    Parameters
    ----------
    text_file : RDD
        A text file (csv) that has been read by spark context
    expected_header : List[str]
        Exact header expected in csv file
    delimiter : str
        Delimiter used in csv file, default as ','

    Returns
    -------
    bool
        Boolean indicator of whether expected and actual headers are identical.
    """
    actual_header = text_file.first()
    buffer = StringIO(actual_header)
    reader = csv.reader(buffer, delimiter=delimiter)
    actual_header = next(reader)
    return expected_header == actual_header


def validate_files(file_paths: Union[str, List[str]], validation_schema: dict, sep: str = ","):
    """
    Validate the header and field count of one or more CSV files on HDFS.

    Parameters
    ----------
    file_paths : Union[str, List[str]]
        One or more paths to files to validate
    validation_schema : dict
        dictionary with ordered keys containing expected column names
    sep : str
        Delimiter used in file, default as ','.

    Raises
    ------
    FileNotFoundError
       If no file path parsed then raise error saying "No file path specified".

    Returns
    -------
    list
       Returns list of file paths for files which have passed the validation check
       and generates a log of file paths and the nature of the error for the files
       which do not pass the validation.
    """
    if file_paths is None or file_paths == "":
        raise FileNotFoundError("No file path specified")
    spark_session = get_or_create_spark_session()
    if file_paths is None or file_paths in ["", []]:
        raise FileNotFoundError("No file path specified")
    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    expected_header_row = list(validation_schema.keys())

    valid_files = []
    for file_path in file_paths:
        if Path(file_path).suffix in [".xlsx"]:  # TODO: add validation of xl files using pandas reading to get the
            valid_files.append(file_path)
            continue
        error = ""
        text_file = spark_session.sparkContext.textFile(file_path)
        valid_csv_header = validate_csv_header(text_file, expected_header_row, delimiter=sep)
        valid_csv_fields = validate_csv_fields(text_file, delimiter=sep)

        if not valid_csv_header:
            actual_header = text_file.first()
            buffer = StringIO(actual_header)
            reader = csv.reader(buffer, delimiter=sep)
            actual_header = next(reader)
            expected_header = list(validation_schema.keys())
            error += (
                f"Invalid file header:{file_path}\n"
                f"Expected:     {expected_header}\n"
                f"Actual:       {actual_header}\n"
                f"Missing:      {set(expected_header) - set(actual_header)}\n"
                f"Additional:   {set(actual_header) - set(expected_header)}\n"
            )

        if not valid_csv_fields:
            error += (
                f"\nInvalid file: Number of fields in {file_path} "
                "row(s) does not match expected number of columns from header"
            )

        if error != "":
            print(error)  # functional
            add_error_file_log_entry(file_path, error)
        else:
            valid_files.append(file_path)
    return valid_files


def validate_processed_files(df: DataFrame, source_file_column: str):
    """
    Check that all of the files processed in a combined dataframe exist in the folder from which they were found.

    Parameters
    ----------
    df : DataFrame
        Combined dataframe to be checked
    source_file_column: str
        ???

    Returns
    -------
    list, list, list
        List of processed files, list of unprocessed files and list of files that have been processed
        but no longer exist.
    """
    processed_files = column_to_distinct_list(df, source_file_column)
    processed_files = [f for f in processed_files if isinstance(f, str) and f != ""]
    dirs = [Path(f).parent.as_posix() for f in processed_files]
    dirs = [d for d in dirs if d not in ["."]]
    dirs = [d for d in dirs if isinstance(d, str)]
    found_files = []
    for d in dirs:
        files = [f for f in list_contents(d, date_from_filename=False)["file_path"].to_list() if isinstance(f, str)]
        found_files.extend(files)
    found_files = sorted(found_files)
    unprocessed = sorted(set(found_files) - set(processed_files))
    non_existent = sorted(set(processed_files) - set(found_files))
    if unprocessed:
        u = "\n".join(unprocessed)
        print(f"Found {len(unprocessed)} unprocessed files: {u}")  # functional
    if non_existent:
        n = "\n".join(non_existent)
        print(f"Found {len(non_existent)} files that have been processed but no longer exist: {n}")  # functional
    return processed_files, unprocessed, non_existent


# This function does not appear to be used n any other scripts in the project!
def check_singular_match(
    df: DataFrame,
    drop_flag_column_name: str,
    failure_column_name: str,
    group_by_columns: List[str],
    existing_failure_column: str = None,
):
    """
    Given a set of columns related to the final drop flag of a given merge function on the complete
    (merged) dataframe produce an indication column (failure column) which stipulates whether the
    merge function has returned a unique match

    Parameters
    ----------
    df : DataFrame
        The merged dataframe to be executed on.
    drop_flag_column_name: str,
        Column with final flag from merge function to be dropped(??)
    failure_column_name : str
        Column in which to store bool flag that shows if singular match occurred for given merge
    group_by_columns : List[str]
        List of columns to check is singular given criteria
    existing_failure_column : str
        ??? Default is None.

    Returns
    -------
    DataFrame
        Input dataframe with additional column added indicating whether the merge function has returned
        a unique match, with new column header given by failure_column_name.
    """
    if type(group_by_columns) != list:
        group_by_columns = [group_by_columns]  # type: ignore
    window = Window.partitionBy(*group_by_columns, drop_flag_column_name)
    df = df.withColumn("TOTAL", F.sum(F.lit(1)).over(window))

    df = df.withColumn(
        failure_column_name, F.when((F.col("total") > 1) & (F.col(drop_flag_column_name).isNull()), 1).otherwise(None)
    )
    if existing_failure_column is not None:
        df = df.withColumn(
            failure_column_name, F.when(F.col(existing_failure_column) == 1, 1).otherwise(F.col(failure_column_name))
        )
    return df.drop("TOTAL")


class ConfigError(Exception):
    pass


def validate_config_stages(pipeline_stage_functions: Dict, stages_to_run: List[str], stages_config: List):
    """
    Checks that there's a valid input in the pipeline_config.yaml for every stage input argument.

    Parameters
    ----------
    all_function_dict : dict
       Dictionary of all functions name and function object in pipeline_stages.py
    stages_to_run: List[str]
       List from the config file all the functions that have been set up to run. (??)
    stages_config: List
       ???

    Raises
    -------
    ConfigError
        Custom error message (error_msg), built up by variety of checks highlighting any failures.
    """
    error_msg = "\n"
    optional_parameters = ["function", "input_survey_table", "input_stage"]
    # CHECK: function in config file exists in repo
    for stage_name in stages_to_run:
        # check that theres an object function per each of the pipeline stages in the config_file
        function_name = stages_config[stage_name].get("function", stage_name)  # type: ignore
        if function_name not in set(list(pipeline_stage_functions.keys())):
            error_msg += f"""  - the {function_name} stage function isn't defined. \n"""  # noqa: E501

        config_arguments_dict = {
            **stages_config[stage_name].get("input_tables", {}),  # type: ignore
            **stages_config[stage_name].get("io_tables", {}),  # type: ignore
            **stages_config[stage_name].get("output_tables", {}),  # type: ignore
            **{k: v for k, v in stages_config[stage_name].items() if k not in ["input_tables", "io_tables", "output_tables"]},  # type: ignore
        }  # type: ignore
        function_config_other_params = [x for x in config_arguments_dict.keys() if (x not in optional_parameters)]

        # CHECK: for stage function that require when,
        # ensure operator and condition exist and stages required are turned on.
        if "when" in config_arguments_dict:
            if type(config_arguments_dict["when"]) == dict:  # operator type and expected value exists
                if not (
                    ("operator" in config_arguments_dict["when"])
                    or (config_arguments_dict["when"]["operator"] == "any")
                    or (config_arguments_dict["when"]["operator"] == "all")
                ):
                    error_msg += (
                        f"""  - {function_name} stage should have operator as either any or all. \n"""  # noqa: E501
                    )
                if "conditions" not in config_arguments_dict["when"]:
                    error_msg += f"""  - {function_name} stage should have conditions as the stages to have been run. \n"""  # noqa: E501
                else:  # there are conditions and the conditions have run as true
                    for function_run_name, status in config_arguments_dict["when"]["conditions"].items():
                        list_needed_functions = [
                            condition_stage
                            for condition_stage in stages_config
                            if ((function_name == function_run_name) and (status == "updated"))
                        ]
                        for function_run_condition in list_needed_functions:
                            error_msg += f"""  - {function_name} stage requires {function_run_condition} stage to be turned as True. \n"""  # noqa: E501
            else:
                error_msg += f"""  - {function_name} stage when condition should be in dictionary format with conditions and operator. \n"""  # noqa: E501

        all_func_config_parameters_from_object = [
            arg
            for arg in inspect.getfullargspec(pipeline_stage_functions[function_name]).args
            if "input_survey_table" not in arg
        ]
        input_arguments_needed = [
            arg
            for arg in all_func_config_parameters_from_object
            if "="  # meaning it will check only non default input parameters
            not in str(inspect.signature(pipeline_stage_functions[function_name]).parameters[arg])
        ]
        if not (set(function_config_other_params) == set(input_arguments_needed)):

            list_not_passed_arg = [x for x in input_arguments_needed if x not in function_config_other_params]
            list_of_unrecognised_arg = [
                x
                for x in function_config_other_params
                if ((x not in all_func_config_parameters_from_object) and (x not in ["when", "kwargs"]))
            ]
            if list_not_passed_arg != []:
                error_msg += f"""  - {function_name} stage does not have in the config file: {', '.join(list_not_passed_arg)}.\n"""  # noqa: E501
            if list_of_unrecognised_arg != []:
                error_msg += f"""  - {function_name} stage has unrecognised input arguments: {', '.join(list_of_unrecognised_arg)}.\n"""  # noqa: E501
    if error_msg != "\n":
        raise ConfigError(error_msg)


def check_lookup_table_joined_columns_unique(df, join_column_list, name_of_df):
    """
    Check for duplicated join keys in lookup table. (???)

    Raises
    ------
    ValueError
        If duplicated join keys are found in lookup dataframe, error message is raised with dataframe name and
        problem rows given.
    """
    duplicate_key_rows_df = df.groupBy(*join_column_list).count().filter("count > 1").drop("count")
    if duplicate_key_rows_df.count() > 0:
        raise ValueError(
            f"The lookup dataframe {name_of_df} has entries with duplicate join keys ({', '.join(join_column_list)})."
            f"Duplicate rows: \n{duplicate_key_rows_df.toPandas()}"
        )
