"""
Common utils for python scripts
"""
import logging
import sys 
from typing import Optional
import omegaconf


def getConfig(path: str) -> omegaconf.dictconfig.DictConfig:
    """
    Read yaml config

    Args:
        path (str): Path to config file

    Returns:
        omegaconf.dictconfig.DictConfig: config object
    """
    config = omegaconf.OmegaConf.load(path)
    return config


def getLogger(
    name: Optional[str] = None,
    level: int=logging.INFO,
    format: str = logging.BASIC_FORMAT,
    date_format: str = None,
    file_name: Optional[str]=None
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
