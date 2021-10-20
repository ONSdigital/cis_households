import os

import yaml


def get_config() -> dict:
    """Read YAML config file from path specified in PIPELINE_CONFIG_LOCATION environment variable"""
    _config_location = os.environ.get("PIPELINE_CONFIG_LOCATION")
    if _config_location is None:
        raise ValueError("PIPELINE_CONFIG_LOCATION environment variable must be set to the config file path")
    with open(_config_location) as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)
    return config


def update_table(df, table_name):
    storage_config = get_config()["storage"]
    df.write.mode(storage_config["write_mode"]).saveAsTable(
        f"{storage_config['database']}.{storage_config['table_prefix']}{table_name}"
    )


def extract_from_table(table_name: str, spark_session):
    storage_config = get_config()["storage"]
    df = spark_session.sql(f"SELECT * FROM {storage_config['database']}.{table_name}")

    return df
