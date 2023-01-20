"""
Common utils for python scripts
"""
import logging
import sys 
import json
from datetime import datetime
from typing import Optional


def getLogger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = logging.BASIC_FORMAT,
    date_format: str = None,
    file_name: Optional[str] = None
    ):
    """
    Get logger with default logging of stdout

    Args:
        name (Optional[str], optional): Name of a logger. Defaults to None.
        level (int, optional): Level of logging. Defaults to logging.INFO.
        format (str, optional): Format of logging message. Defaults to logging.BASIC_FORMAT.
        date_format (str, optional): Format for logging dates. Defaults to None.
        file_name (Optional[str], optional): File to log in. Defaults to None.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # stdout handler
    formatter = logging.Formatter(format, date_format)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    # File handler
    if file_name:
        fh = logging.FileHandler(file_name)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if not name:
        logger.warning("(!) No name was given for logger")

    return logger


class DateTimeEncoder(json.JSONEncoder):
    """
    Class serialize dates to JSON
    """
    def default(self, o) -> json.JSONEncoder.default:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return list(o)
        return json.JSONEncoder.default(self, o)
