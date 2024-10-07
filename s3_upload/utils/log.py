import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
import sys


FORMATTER = logging.Formatter(
    "%(asctime)s [%(module)s] %(levelname)s: %(message)s"
)


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(log_file):
    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=5
    )
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(
    logger_name, log_level=logging.INFO, log_dir="/var/log/s3_upload"
) -> logging.Logger:
    """
    Initialise the logger

    Parameters
    ----------
    logger_name : str
        name of the logger to intialise
    log_level : str
        level of logging to set
    log_dir : str
        path to where to write log files to

    Returns
    -------
    logging.Logger
        handle to configured logger

    Raises
    ------
    AssertionError
        raised when the specified log directory is not writeable if it
        already exists
    """
    if logging.getLogger(logger_name).hasHandlers():
        # logger already exists => use it
        return logging.getLogger(logger_name)

    if os.path.exists(log_dir):
        assert os.access(log_dir, os.W_OK), (
            f"given log directory {log_dir} does not appear to have write"
            " permission"
        )

    log_file = os.path.join(log_dir, "s3_upload.log")

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    Path(log_file).touch(exist_ok=True)

    logger = logging.getLogger(logger_name)

    if log_level:
        logger.setLevel(log_level)

    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler(log_file))

    logger.propagate = False

    logger.info("Initialised log handle, beginning logging to %s", log_file)

    return logger
