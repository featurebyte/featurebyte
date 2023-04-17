"""
Logging formatting
"""
from typing import Any

import sys

from loguru import logger

from featurebyte.common.env_util import is_notebook
from featurebyte.config import Configurations


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
    is_notebook_env = is_notebook()
    if is_notebook_env:
        log_format = (
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<level>{message}</level>"
        )
    else:
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<level>{name}:{function}:{line}</level> | "
            "<level>{message}</level> | {extra}"
        )

    logger_instance.remove()
    sink = sys.stderr if is_notebook() else sys.stderr
    logger_instance.add(
        sink,
        level=configurations.logging.level,
        format=log_format,
        colorize=is_notebook_env,
        serialize=configurations.logging.serialize,
        enqueue=True,  # process logs in background
        diagnose=False,  # hide variable values in log backtrace
    )


configure_logger(logger, Configurations())


__all__ = ["logger", "configure_logger"]
