from datetime import datetime
from logging.config import dictConfig
from pathlib import Path
from typing import Any

import yaml


def configure_logging(verbose: bool, project_dir: str) -> None:
    with Path(__file__).parent.joinpath("log_config.yaml").open(mode="r") as log_config_file:
        log_config = yaml.safe_load(log_config_file)

        logs_dir_path = Path(project_dir).joinpath("logs")
        if logs_dir_path.exists():
            file_handler_name = "logFile"
            log_config["handlers"][file_handler_name] = _get_logging_file_handler(logs_dir_path)
            log_config["loggers"]["nemory"]["handlers"].append(file_handler_name)

        log_config["loggers"]["nemory"]["level"] = "DEBUG" if verbose else "INFO"
        dictConfig(log_config)


def _get_logging_file_handler(logs_dir_path: Path) -> dict[str, Any]:
    return {
        "filename": str(logs_dir_path.joinpath(_get_current_log_filename())),
        "class": "logging.handlers.RotatingFileHandler",
        "formatter": "main",
        "maxBytes": 100000000,  # 100MB
        "backupCount": 12
    }


def _get_current_log_filename() -> str:
    # Creates a new log file every month
    return datetime.now().strftime("log-%Y-%m.txt")
