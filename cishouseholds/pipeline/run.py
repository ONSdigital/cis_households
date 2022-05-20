import time
import traceback
from datetime import datetime

import cishouseholds.pipeline.input_file_stages  # noqa: F401
import cishouseholds.pipeline.pipeline_stages  # noqa: F401
import cishouseholds.pipeline.R_pipeline_stages  # noqa: F401
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import add_run_log_entry
from cishouseholds.pipeline.load import add_run_status
from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pyspark_utils import get_or_create_spark_session
from cishouseholds.pyspark_utils import get_spark_application_id
from cishouseholds.pyspark_utils import get_spark_ui_url


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
    spark = get_or_create_spark_session()
    spark.sparkContext.setCheckpointDir(get_config()["storage"]["checkpoint_directory"])

    config = get_config()
    run_datetime = datetime.now()
    run_id = add_run_log_entry(run_datetime)
    print(f"Run ID: {run_id}")  # functional
    add_run_status(run_id, "started")
    pipeline_error_count = None
    try:
        print(f"Spark UI: {get_spark_ui_url()}")  # functional
        print(f"Spark application ID: {get_spark_application_id()}")  # functional
        pipeline_stage_list = [stage for stage in config["stages"] if stage.pop("run")]
        pipeline_error_count = run_pipeline_stages(
            pipeline_stage_list, run_id, config.get("retry_times_on_fail", 0), config.get("retry_wait_time_seconds", 0)
        )
    except Exception as e:
        add_run_status(run_id, "errored", "run_from_config", "\n".join(traceback.format_exc()))
        raise e
    run_time = (datetime.now() - run_datetime).total_seconds()
    print(f"\nPipeline run completed in: {run_time//60:.0f} minute(s) and {run_time%60:.1f} second(s)")  # functional
    if pipeline_error_count != 0:
        add_run_status(run_id, "finished with errors")
        raise ValueError(f"Pipeline finished with {pipeline_error_count} stage(s) erroring.")
    add_run_status(run_id, "finished")


def run_pipeline_stages(pipeline_stage_list: list, run_id: int, retry_count: int = 1, retry_wait_time: int = 1):
    """Run each stage of the pipeline. Catches, prints and logs any errors, but continues the pipeline run."""
    number_of_stages = len(pipeline_stage_list)
    max_digits = len(str(number_of_stages))
    pipeline_error_count = 0
    for n, stage_config in enumerate(pipeline_stage_list):
        stage_start = datetime.now()
        stage_success = False
        attempt = 0
        stage_name = stage_config.pop("function")
        stage_text = f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_name}"
        print(stage_text)  # functional
        while not stage_success and attempt < retry_count + 1:
            # TODO: log the retry event with run status and no traceback
            if attempt != 0:
                add_run_status(run_id, "retry", stage_text, "")
            attempt_start = datetime.now()
            try:
                pipeline_stages[stage_name](**stage_config)
                stage_success = True
                add_run_status(run_id, "success", stage_text, "")
            except Exception:
                attempt_run_time = (datetime.now() - attempt_start).total_seconds()
                print(f"Error: {traceback.format_exc()}")  # functional
                print(
                    f"    - attempt {attempt} ran for {attempt_run_time//60:.0f} minute(s) and {attempt_run_time%60:.1f} second(s)"  # noqa:E501
                )  # functional
                add_run_status(run_id, "errored", stage_text, "\n".join(traceback.format_exc()))
            attempt += 1
            time.sleep(retry_wait_time)
        if not stage_success:
            pipeline_error_count += 1
        stage_run_time = (datetime.now() - stage_start).total_seconds()
        print(
            f"    - completed in: {stage_run_time//60:.0f} minute(s) and {stage_run_time%60:.1f} second(s) in {attempt} attempt(s)"  # noqa:E501
        )  # functional
    return pipeline_error_count


if __name__ == "__main__":
    run_from_config()
