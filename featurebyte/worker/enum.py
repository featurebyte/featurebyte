"""
This module contains all the enums used in workder directory.
"""
from enum import Enum


class Command(str, Enum):
    """
    Task command enum
    """

    DEFAULT_JOB_SETTINGS_ANALYSIS = "DEFAULT_JOB_SETTINGS_ANALYSIS"
