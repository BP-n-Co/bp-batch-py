import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

root_path = Path(__file__).resolve()
sys.path.append(str(root_path))

load_dotenv(override=False)

base_logger = logging.getLogger(name="BP_logger")


class DateTimeFormat:
    github = "%Y-%m-%dT%H:%M:%SZ"
    bp_co_long = "%Y-%m-%d %H:%M:%S"


class ServiceEnv:
    local = "local"
    staging = "staging"
    production = "production"


ENV = os.getenv("ENV", "local")

MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


class LocalFormatter(logging.Formatter):
    def format(self, record):
        LOG_COlORS = {
            "DEBUG": "\033[94m",  # Blue
            "INFO": "\033[92m",  # Green
            "WARNING": "\033[93m",  # Yellow
            "ERROR": "\033[91m",  # Red
            "CRITICAL": "\033[41;97m",  # White on red
        }
        RESET_COLOR = "\033[0m"

        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        level = record.levelname
        color = LOG_COlORS.get(level, "")
        logger = record.name
        message = record.getMessage()

        log_record = (
            f"\n{color}{timestamp}\n{level} from {logger}\n{message}{RESET_COLOR}"
        )

        return log_record


def get_logger(name="BP_logger", env: str = ENV):
    logger = logging.getLogger(name)
    if env == ServiceEnv.production:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        if env == ServiceEnv.production:
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(LocalFormatter())
        logger.addHandler(handler)

    return logger


base_logger = get_logger()
