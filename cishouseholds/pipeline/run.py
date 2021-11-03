import traceback
from datetime import datetime

import cishouseholds.pipeline.blood_delta_ETL  # noqa: F401
import cishouseholds.pipeline.merge_antibody_swab_ETL  # noqa: F401
import cishouseholds.pipeline.survey_responses_version_2_ETL  # noqa: F401
import cishouseholds.pipeline.swab_delta_ETL  # noqa: F401
from cishouseholds.pipeline.config import get_config
from cishouseholds.pipeline.load import add_run_log_entry
from cishouseholds.pipeline.load import add_run_status
from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pipeline.post_merge_processing import process_post_merge  # noqa: F401


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
    try:
        config = get_config()
        run_datetime = datetime.now()
        run_id = add_run_log_entry(config, run_datetime)
        print(f"Run ID: {run_id}")  # functional
        add_run_status(run_id, "started")

        pipeline_stage_list = [stage for stage in config["stages"] if stage.pop("run")]
        run_pipeline_stages(pipeline_stage_list, config, run_id)
    except Exception as e:
        add_run_status(run_id, "errored", "run_from_config", "\n".join(traceback.format_exc()))
        raise e

    add_run_status(run_id, "finished")


def run_pipeline_stages(pipeline_stage_list: list, config: dict, run_id: int):
    """Run each stage of the pipeline. Catches, prints and logs any errors, but continues the pipeline run."""
    number_of_stages = len(pipeline_stage_list)
    max_digits = len(str(number_of_stages))
    for n, stage_config in enumerate(pipeline_stage_list):
        try:
            stage_name = stage_config.pop("function")
            stage_text = f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_name}"
            print(stage_text)  # functional
            output_df = pipeline_stages[stage_name](**stage_config)
            output_df.toPandas().to_csv(
                f"{config['csv_output_path']}/{stage_name}_output_{datetime.now().strftime('%y%m%d_%H%M%S')}.csv",
                index=False,
            )
        except Exception:
            add_run_status(run_id, "errored", stage_text, "\n".join(traceback.format_exc()))
            print(f"Error: {traceback.format_exc()}")  # functional


if __name__ == "__main__":
    run_from_config()
