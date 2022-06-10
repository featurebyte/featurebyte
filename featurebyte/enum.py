"""
This module contains all the enums used across different modules
"""
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


class SourceType(str, Enum):
    """
    Database or data warehouse source type
    """

    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
