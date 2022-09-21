"""
Logging formatting
"""
from typing import Any

import sys

from loguru import logger

from featurebyte.config import Configurations

LOGGER_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<level>{name}:{function}:{line}</level> | "
    "<level>{message}</level> | {extra}"
)

config = Configurations()


def configure_logger(logger_instance: Any, configurations: Configurations) -> None:
    """
    Update logger

    Parameters
    ----------
    logger_instance: Any
        Logger object to update
    configurations: Configurations
        configurations used to update logger
    """
    logger_instance.remove()
    logger_instance.add(
        sys.stderr,
        level=configurations.logging.level,
        format=LOGGER_FORMAT,
        serialize=configurations.logging.serialize,
        enqueue=True,  # process logs in background
        diagnose=False,  # hide variable values in log backtrace
    )


configure_logger(logger, config)


__all__ = ["logger", "configure_logger"]
