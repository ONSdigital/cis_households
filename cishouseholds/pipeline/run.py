from datetime import datetime

import cishouseholds.pipeline.blood_delta_ETL  # noqa: F401
import cishouseholds.pipeline.survey_responses_version_2_ETL  # noqa: F401
import cishouseholds.pipeline.swab_delta_ETL  # noqa: F401
from cishouseholds.pipeline.load import get_config
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
    config = get_config()
    run_stages = [stage for stage in config["stages"] if stage.pop("run")]
    number_of_stages = len(run_stages)
    max_digits = len(str(number_of_stages))
    for n, stage_config in enumerate(run_stages):
        stage_name = stage_config.pop("function")
        print(f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_name}")  # functional
        output_df = pipeline_stages[stage_name](**stage_config)
        output_df.toPandas().to_csv(
            f"{config['csv_output_path']}/{stage_name}_output_{datetime.now().strftime('%y%m%d_%H%M%S')}.csv",
            index=False,
        )


if __name__ == "__main__":
    run_from_config()
