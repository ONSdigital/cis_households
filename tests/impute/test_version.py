import configparser
import os
from pathlib import Path


def test_version():

    config = configparser.ConfigParser()
    config_file = Path(__file__).parent.parent.parent / ".bumpversion.cfg"
    config.readfp(open(config_file))
    pipeline_version = config.get("bumpversion", "current_version")
