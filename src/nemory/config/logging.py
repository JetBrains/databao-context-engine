from datetime import datetime
from logging.config import dictConfig
from pathlib import Path
from typing import Any

import yaml

from nemory.project.layout import get_logs_dir, is_project_dir_valid


def configure_logging(verbose: bool, quiet: bool, project_dir: Path) -> None:
    with Path(__file__).parent.joinpath("log_config.yaml").open(mode="r") as log_config_file:
        log_config = yaml.safe_load(log_config_file)

        if is_project_dir_valid(project_dir):
            logs_dir_path = get_logs_dir(project_dir)
            logs_dir_path.mkdir(exist_ok=True)

            file_handler_name = "logFile"
            log_config["handlers"][file_handler_name] = _get_logging_file_handler(logs_dir_path)
            log_config["loggers"]["nemory"]["handlers"].append(file_handler_name)

        if quiet:
            log_config["loggers"]["nemory"]["handlers"].remove("console")
        if verbose:
            log_config["loggers"]["nemory"]["level"] = "DEBUG"

        dictConfig(log_config)


def _get_logging_file_handler(logs_dir_path: Path) -> dict[str, Any]:
    return {
        "filename": str(logs_dir_path.joinpath(_get_current_log_filename())),
        "class": "logging.handlers.RotatingFileHandler",
        "formatter": "main",
        "maxBytes": 100000000,  # 100MB
        "backupCount": 12,
    }


def _get_current_log_filename() -> str:
    # Creates a new log file every month
    return datetime.now().strftime("log-%Y-%m.txt")
