"""
Contains all the enums used for execution graph construction
"""
from enum import Enum


class NodeType(str, Enum):

    # logical expressions
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    # relational expressions
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GE = "GE"
    LT = "LT"
    LE = "LE"

    # arithmetic expressions
    ADD = "ADD"
    SUB = "SUB"
    MUL = "MUL"
    DIV = "DIV"
    MOD = "MOD"

    # SQL operations
    PROJECT = "PROJECT"
    FILTER = "FILTER"

    # operations with side effect
    INPUT = "INPUT"


class NodeOutputType(str, Enum):

    DataFrame = "DataFrame"
    Series = "Series"
