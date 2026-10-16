"""Configuration helpers shared by the command-line tools and Prefect flows."""

import configparser
import os
from pathlib import Path


def load_config() -> configparser.ConfigParser:
    config_path = os.getenv("FLOCS_CONFIG") or os.path.expanduser("~/.flocs.cfg")
    path = Path(config_path)
    if not path.is_file():
        raise RuntimeError(
            "Set FLOCS_CONFIG to a valid configuration file or create ~/.flocs.cfg."
        )

    parser = configparser.ConfigParser()
    parser.optionxform = str
    with path.open() as config_file:
        parser.read_string("[DEFAULT]\n" + config_file.read())
    return parser
