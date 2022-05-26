"""
This module contains all the enums used across different modules
"""
from enum import Enum


class DBVarType(str, Enum):
    """
    Database variable type
    """

    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    INT = "INT"
    DATE = "DATE"
    FLOAT = "FLOAT"
    BOOL = "BOOL"
