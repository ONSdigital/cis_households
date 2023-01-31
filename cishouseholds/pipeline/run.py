import inspect
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from typing import Dict
from typing import List

import cishouseholds.pipeline.input_file_stages  # noqa: F401
import cishouseholds.pipeline.pipeline_stages  # noqa: F401
from cishouseholds.hdfs_utils import cleanup_checkpoint_dir
from cishouseholds.log import SplunkLogger
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import add_run_log_entry
from cishouseholds.pipeline.load import add_run_status
from cishouseholds.pipeline.load import check_table_exists
from cishouseholds.pipeline.logging import check_survey_table_lengths
from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.pyspark_utils import get_spark_application_id
from cishouseholds.pyspark_utils import get_spark_ui_url
from cishouseholds.validate import validate_config_stages


class MissingTablesError(Exception):
    pass


@contextmanager
def spark_description_set(description):
    spark = get_or_create_spark_session()
    spark.sparkContext.setJobDescription(description)
    try:
        yield
    finally:
        spark.sparkContext.setJobDescription(None)


def check_conditions(stage_responses: dict, stage_config: dict):
    if "when" not in stage_config:
        return True
    elif stage_config["when"]["operator"] == "all":
        return all(stage_responses[stage] == status for stage, status in stage_config["when"]["conditions"].items())
    elif stage_config["when"]["operator"] == "any":
        return any(stage_responses[stage] == status for stage, status in stage_config["when"]["conditions"].items())
    return False


def check_dependencies(stages_to_run, stages_config):  # TODO: ensure check in order. look before current stage only

    available_tables = []
    for stage in stages_to_run:  # generate available and required tables from stage config
        required_tables = []
        input_tables = stages_config[stage].get("input_tables", {})

        function = stages_config[stage].get("function", stage)

        if type(input_tables) == dict:
            required_tables.extend(input_tables.values())
        elif type(input_tables) == list:
            required_tables.extend(input_tables)
        if "tables_to_process" in stages_config[stage]:
            required_tables.extend(stages_config[stage]["tables_to_process"])

        unavailable_tables = [
            table for table in required_tables if not check_table_exists(table)
        ]  # remove existing tables
        unavailable_tables = [
            table for table in unavailable_tables if table not in available_tables
        ]  # remove tables that will be created
        optional_input_args = [
            arg
            for arg in inspect.getfullargspec(pipeline_stages[function]).args
            if "="  # meaning it will check only non default input parameters
            in str(inspect.signature(pipeline_stages[function]).parameters[arg])
        ]
        unavailable_tables = [table for table in unavailable_tables if table not in optional_input_args]
        missing_tables = ",".join(unavailable_tables)

        if len(unavailable_tables) > 0:
            raise MissingTablesError(f"Cannot run pipeline; stage {stage} is missing tables: {missing_tables}")

        available_tables.extend(stages_config[stage].get("output_tables", {}).values())
        if "dataset_name" in stages_config[stage]:  # handle auto name delta tables
            available_tables.append(f"transformed_{stages_config[stage]['dataset_name']}")


def run_from_config():
    """
    Run ordered pipeline stages, from pipeline configuration. Config file location must be specified in the environment
    variable ``PIPELINE_CONFIG_LOCATION``.
    ``function`` and ``run`` are essential keys for each stage. All other key value pairs are passed to the function.
    An example stage is configured:
    stages:
    - function: process_csv
      run: True
      resource_path: "path_to.csv"
    """
    os.environ["deployment"] = "hdfs"

    spark = get_or_create_spark_session()
    config = get_config()
    stages_to_run = config["run"]

    spark.sparkContext.setCheckpointDir(config["storage"]["checkpoint_directory"])

    check_dependencies(stages_to_run, config["stages"])

    validate_config_stages(
        pipeline_stage_functions=pipeline_stages,
        stages_to_run=stages_to_run,
        stages_config=config["stages"],
    )

    pipeline_version = cishouseholds.__version__

    run_datetime = datetime.now()
    splunk_logger = SplunkLogger(config.get("splunk_log_directory"))

    with spark_description_set("adding run log entry"):
        run_id = add_run_log_entry(run_datetime)
    print(f"Run ID: {run_id}")  # functional
    with spark_description_set("adding run status"):
        add_run_status(run_id, "started")
    pipeline_error_count = None

    try:
        print(f"Spark UI: {get_spark_ui_url()}")  # functional
        print(f"Spark application ID: {get_spark_application_id()}")  # functional
        print(f"cishouseholds version number: {pipeline_version}")  # functional
        splunk_logger.log(status="start")

        pipeline_error_count = run_pipeline_stages(
            stages_to_run,
            config["stages"],
            run_id,
            splunk_logger,
            config.get("retry_times_on_fail", 0),
            config.get("retry_wait_time_seconds", 0),
        )
    except Exception as e:
        exception_text = traceback.format_exc()
        with spark_description_set("adding run status"):
            add_run_status(run_id, "errored", "run_from_config", exception_text)
        splunk_logger.log(
            status="error",
            error_stage="run_from_config",
            error_message=repr(e),
        )
        raise e
    finally:
        # clean up check-pointed files
        cleanup_checkpoint_dir(spark)

    run_time = (datetime.now() - run_datetime).total_seconds()

    try:
        check_survey_table_lengths()
    except ValueError as e:
        add_run_status(run_id, "Error in cleanup", "cleanup", e)
        pipeline_error_count += 1

    print(f"\nPipeline run completed in: {run_time//60:.0f} minute(s) and {run_time%60:.1f} second(s)")  # functional
    if pipeline_error_count != 0:
        with spark_description_set("adding run status"):
            add_run_status(run_id, "finished with errors")
        splunk_logger.log(status="failure", stage_error_count=pipeline_error_count)
        raise ValueError(f"Pipeline finished with {pipeline_error_count} stage(s) erroring.")
    with spark_description_set("adding run status"):
        add_run_status(run_id, "finished")

    splunk_logger.log(status="success")


def run_pipeline_stages(
    pipeline_stage_list: List[str],
    stage_configs: dict,
    run_id: int,
    splunk_logger: SplunkLogger,
    retry_count: int = 1,
    retry_wait_time: int = 1,
):
    """
    Run each stage of the pipeline. Catches, prints and logs any errors, but continues the pipeline run.
    Any failing stages will be retried `retry_times_on_fail` times as set in the config file.
    Whether a stage runs can also be set conditionally by using
    when, operator, condition configuration in the config file.

    Example
    -------------
    - function: example
      run: True
      when:
        operator: all (possible values All, Any)
        conditions:
            (a stage name that returns a status string): the desired status string
            stage_A: updated
            stage_B: updated

    Notes
    -------------
    Ensure that the stages referenced in any condition do return a status.
    A status can be added by adding a return string to the stage function.
    """

    number_of_stages = len(pipeline_stage_list)
    max_digits = len(str(number_of_stages))
    pipeline_error_count = 0
    stage_responses: Dict[str, str] = {}
    current_table = None

    for n, stage_name in enumerate(pipeline_stage_list):
        stage_config = stage_configs[stage_name]
        stage_config = {} if stage_config is None else stage_config
        stage_function_name = stage_config.pop("function", stage_name)
        stage_function_args = inspect.getfullargspec(pipeline_stages[stage_function_name]).args

        stage_start = datetime.now()
        stage_success = False
        attempt = 0
        complete_status_string = "successfully"
        stage_text = f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_name} at {stage_start}"

        stage_input_tables = stage_config.pop("input_tables", {})
        stage_output_tables = stage_config.pop("output_tables", {})

        stage_config.update(stage_input_tables)
        stage_config.update(stage_output_tables)

        print(stage_text)  # functional
        if check_conditions(stage_responses=stage_responses, stage_config=stage_config):
            stage_config.pop("when", None)
            if (  # try to add input survey table directly
                current_table is not None
                and "input_survey_table" in stage_function_args
                and "input_survey_table" not in stage_config
            ):  # automatically add input table name
                stage_config["input_survey_table"] = current_table

            elif (  # try to add input survey table from input stage
                "input_stage" in stage_config
                and stage_config["input_stage"] in stage_configs
                and "output_tables" in stage_configs[stage_config["input_stage"]]
                and "input_survey_table" in stage_function_args
            ):
                stage_config["input_survey_table"] = stage_configs[stage_config["input_stage"]]["output_tables"][
                    "output_survey_table"
                ]
            if "input_stage" in stage_config:
                del stage_config["input_stage"]

            while not stage_success and attempt < retry_count + 1:
                if attempt != 0:
                    with spark_description_set("adding run status"):
                        add_run_status(run_id, "retry", stage_text, "")
                attempt_start = datetime.now()
                try:
                    with spark_description_set(stage_name):
                        result = pipeline_stages[stage_function_name](**stage_config)
                        if result is not None:
                            stage_responses[stage_name] = result.get("status")
                            current_table = result.get("output_survey_table", current_table)
                    stage_success = True
                    with spark_description_set("adding run status"):
                        add_run_status(run_id, "success", stage_text, "")
                except Exception as e:
                    exception_text = traceback.format_exc()
                    attempt_run_time = (datetime.now() - attempt_start).total_seconds()
                    print(exception_text)  # functional
                    print(
                        f"    - attempt {attempt} ran for {attempt_run_time//60:.0f} minute(s) and {attempt_run_time%60:.1f} second(s)"  # noqa:E501
                    )  # functional

                    print(exception_text)  # functional
                    with spark_description_set("adding run status"):
                        add_run_status(run_id, "errored", stage_text, exception_text)
                    splunk_logger.log(
                        status="error",
                        error_stage=stage_name,
                        error_message=repr(e),
                    )

                attempt += 1
                time.sleep(retry_wait_time)
            if not stage_success:
                pipeline_error_count += 1
                complete_status_string = "unsuccessfully"
            stage_run_time = (datetime.now() - stage_start).total_seconds()
            print(
                f"    - completed {complete_status_string} in: {stage_run_time//60:.0f} minute(s) and {stage_run_time%60:.1f} second(s) in {attempt} attempt(s)"  # noqa:E501
            )  # functional
        else:
            print("    - stage not run")  # functional
    return pipeline_error_count


if __name__ == "__main__":
    run_from_config()
