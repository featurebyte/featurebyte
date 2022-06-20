"""
Logging formatting
"""
import sys

from loguru import logger

from featurebyte.config import config

LOGGER_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<level>{message}</level> | {extra}"
)

logger.remove()
logger.add(
    sys.stderr,
    level=config.logging.level.upper(),
    format=LOGGER_FORMAT,
    serialize=config.logging.serialize,
    enqueue=True,  # process logs in background
    diagnose=False,  # hide variable values in log backtrace
)

__all__ = ["logger"]
