"""
Main

This module is the entry point for the application.
"""
import sys
import traceback
import configparser
import logging

from src.pipeline import Pipeline

LOG_FILENAME = "config/logging.json"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    """Run the app."""
    config = read_config_file("config/app.config")
    name = config["job_logger"]["name"]
    project = config["job_logger"]["project"]
    credential = {
        "host": config["postgres_credential"]["host"],
        "database": config["postgres_credential"]["database"],
        "user": config["postgres_credential"]["user"],
        "password": config["postgres_credential"]["password"],
    }
    logger.info(f"Start running Python script to {name}. For {project}.")

    pipeline = Pipeline(credentials=credential)
    pipeline.run()


def read_config_file(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Caught unhandled exception while running main.")

        failure_message = "".join(traceback.format_exception(*sys.exc_info()))
        sys.exit(1)

    print("Finished running the python script.")
