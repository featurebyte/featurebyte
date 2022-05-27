"""
This module contains all the enums used across different modules
"""
from enum import Enum


class DBVarType(str, Enum):
    """
    Database variable type
    """

    BOOL = "BOOL"
    CHAR = "CHAR"
    DATE = "DATE"
    FLOAT = "FLOAT"
    INT = "INT"
    VARCHAR = "VARCHAR"
