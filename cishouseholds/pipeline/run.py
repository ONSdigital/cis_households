import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from typing import Dict
from typing import List

import cishouseholds.pipeline.input_file_stages  # noqa: F401
import cishouseholds.pipeline.pipeline_stages  # noqa: F401
import cishouseholds.pipeline.R_pipeline_stages  # noqa: F401
from cishouseholds.log import SplunkLogger
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import add_run_log_entry
from cishouseholds.pipeline.load import add_run_status
from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.pyspark_utils import get_spark_application_id
from cishouseholds.pyspark_utils import get_spark_ui_url
from cishouseholds.validate import validate_config_stages


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


def run_from_config(config_file_path: str = None):
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
    spark = get_or_create_spark_session()
    spark.sparkContext.setCheckpointDir(get_config()["storage"]["checkpoint_directory"])
    if config_file_path is not None:
        os.environ["PIPELINE_CONFIG_LOCATION"] = config_file_path
    config = get_config()

    run_datetime = datetime.now()
    splunk_logger = SplunkLogger(config.get("splunk_log_directory"))
    with spark_description_set("adding run log entry"):
        run_id = add_run_log_entry(run_datetime)
    print(f"Run ID: {run_id}")  # functional
    with spark_description_set("adding run status"):
        add_run_status(run_id, "started")
    pipeline_error_count = None

    try:
        validate_config_stages(all_object_function_dict=pipeline_stages, config_arguments_list_of_dict=config["stages"])
        pipeline_stage_list = [stage for stage in config["stages"] if stage.pop("run")]
        print(f"Spark UI: {get_spark_ui_url()}")  # functional
        print(f"Spark application ID: {get_spark_application_id()}")  # functional
        splunk_logger.log(status="start")

        pipeline_error_count = run_pipeline_stages(
            pipeline_stage_list,
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
    run_time = (datetime.now() - run_datetime).total_seconds()
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
    pipeline_stage_list: List[dict],
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
    for n, stage_config in enumerate(pipeline_stage_list):
        stage_start = datetime.now()
        stage_success = False
        attempt = 0
        complete_status_string = "successfully"
        stage_name = stage_config.pop("function")
        stage_text = f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_name}"
        print(stage_text)  # functional
        if check_conditions(stage_responses=stage_responses, stage_config=stage_config):
            stage_config.pop("when", None)
            while not stage_success and attempt < retry_count + 1:
                if attempt != 0:
                    with spark_description_set("adding run status"):
                        add_run_status(run_id, "retry", stage_text, "")
                attempt_start = datetime.now()
                try:
                    with spark_description_set(stage_name):
                        stage_responses[stage_name] = pipeline_stages[stage_name](**stage_config)
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
