"""
This module contains all the enums used across different modules
"""
from __future__ import annotations

from enum import Enum


class DBVarType(str, Enum):
    """
    Database variable type
    """

    BINARY = "BINARY"
    BOOL = "BOOL"
    CHAR = "CHAR"
    DATE = "DATE"
    FLOAT = "FLOAT"
    INT = "INT"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    VARCHAR = "VARCHAR"


class AggFunc(str, Enum):
    """
    Supported aggregation functions in groupby
    """

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    NA_COUNT = "na_count"

    @classmethod
    def all(cls) -> list[str]:
        """List all defined aggregation function names

        Returns
        -------
        list[str]
        """
        return [c.value for c in cls]
