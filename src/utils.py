"""Common utils for python"""
import logging
import sys 
from typing import Optional


def getLogger(
    name: Optional[str] = None,
    level: int=logging.INFO,
    format: str = logging.BASIC_FORMAT,
    file_name: Optional[str]=None
    ):
    """
    Get logger with default logging of stdout

    Args:
        name (Optional[str], optional): Name of a logger. Defaults to None.
        level (int, optional): Level of logging. Defaults to logging.INFO.
        format (str, optional): Format of logging. Defaults to logging.BASIC_FORMAT.
        file_name (Optional[str], optional): File to log in. Defaults to None.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(format)

    # stdout handler
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
