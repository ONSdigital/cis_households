import cishouseholds.pipeline.bloods_delta_ETL  # noqa: F401
import cishouseholds.pipeline.survey_responses_version_2_ETL  # noqa: F401
import cishouseholds.pipeline.swab_delta_ETL  # noqa: F401
from cishouseholds import config
from cishouseholds.pipeline.pipeline_stages import pipeline_stages
from cishouseholds.pipeline.post_merge_processing import process_post_merge  # noqa: F401


def run_from_config():
    """
    Reads yaml config file containing variables (run, function and resource path) per ETL function
    requires setting of PIPELINE_CONFIG_LOCATION environment var with file path of output
    """
    run_stages = [stage for stage in config["stages"] if stage.pop("run")]
    number_of_stages = len(run_stages)
    max_digits = len(str(number_of_stages))
    for n, stage_config in enumerate(run_stages):
        print(f"Stage {n + 1 :0{max_digits}}/{number_of_stages}: {stage_config['function']}")  # functional
        pipeline_stages[stage_config.pop("function")](**stage_config)


if __name__ == "__main__":
    run_from_config()
