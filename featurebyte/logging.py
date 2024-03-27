"""
Logging formatting
"""
from __future__ import annotations

from typing import Any, Mapping

import logging
import os
import sys

from featurebyte.common.env_util import is_notebook
from featurebyte.config import Configurations


class CustomLogger(logging.Logger):
    """
    Custom logger to capture extra field
    """

    def makeRecord(  # pylint: disable=too-many-arguments
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: object,
        args: Any,
        exc_info: Any,
        func: Any = None,
        extra: Mapping[str, object] | None = None,
        sinfo: Any = None,
    ) -> logging.LogRecord:
        record = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        record.extra = extra or {}
        return record


logging.setLoggerClass(CustomLogger)


class CustomFormatter(logging.Formatter):
    """
    Custom logging formatter
    """

    colors = {
        "red": "\x1b[31;20m",
        "green": "\x1b[32;20m",
        "yellow": "\x1b[33;20m",
        "blue": "\x1b[34;20m",
        "grey": "\x1b[38;20m",
        "bold_red": "\x1b[31;1m",
    }

    level_colors = {
        "DEBUG": "blue",
        "INFO": "grey",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red",
    }

    @classmethod
    def colorize(cls, value: str, color: str) -> str:
        """
        Colorize string

        Parameters
        ----------
        value: str
            String to colorize
        color: str
            Color to use

        Returns
        -------
        str
            Colorized string
        """
        if color not in cls.colors:
            return value
        color_code = cls.colors[color]
        return f"{color_code}{value}\x1b[0m"

    def format(self, record: logging.LogRecord) -> str:
        record.msg = self.colorize(record.msg, self.level_colors.get(record.levelname, "grey"))
        record.levelname = self.colorize(
            record.levelname.ljust(8), self.level_colors.get(record.levelname, "grey")
        )
        return super().format(record)


NOTEBOOK_LOG_FORMATTER = CustomFormatter(
    "\x1b[32;20m%(asctime)s\x1b[0m | \033[1m%(levelname)s\x1b[0m | \033[1m%(message)s\x1b[0m",
    "%H:%M:%S",
)
CONSOLE_LOG_FORMATTER = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s | %(extra)s",
    "%Y-%m-%d %H:%M:%S",
)


def set_logger_level(logger: logging.Logger, configurations: Configurations) -> None:
    """
    Set logger level

    Parameters
    ----------
    logger: logging.Logger
        Logger to set level
    configurations: Configurations
        Optional configurations used to configure logger
    """
    if os.environ.get("LOG_LEVEL"):
        logger.setLevel(os.environ.get("LOG_LEVEL"))  # type: ignore[arg-type]
    else:
        logger.setLevel(configurations.logging.level)


def get_logger(logger_name: str, configurations: Configurations | None = None) -> logging.Logger:
    """
    Get logger

    Parameters
    ----------
    logger_name: str
        Name of logger
    configurations: Configurations
        Optional configurations used to configure logger

    Returns
    -------
    logging.Logger
    """
    _ = configurations
    return logging.getLogger(logger_name)


def reconfigure_loggers(configurations: Configurations) -> None:
    """
    Reconfigure all loggers with configurations.

    Parameters
    ----------
    configurations: Configurations
        Configurations to use
    """
    configure_featurebyte_logger(configurations)


def configure_featurebyte_logger(configurations: Configurations) -> None:
    """
    Configure featurebyte logger
    """
    configurations = configurations or Configurations()
    is_notebook_env = is_notebook()
    formatter: logging.Formatter = CONSOLE_LOG_FORMATTER
    if is_notebook_env:
        formatter = NOTEBOOK_LOG_FORMATTER

    console_handler = logging.StreamHandler(stream=sys.stderr)
    console_handler.setFormatter(formatter)
    logger = logging.getLogger("featurebyte")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(console_handler)
    set_logger_level(logger, configurations)


__all__ = ["get_logger"]
