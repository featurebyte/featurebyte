"""
This module contains all the enums used for query graph.
"""
from enum import Enum


class NodeType(str, Enum):
    """
    Query graph node type
    """

    # logical expressions
    AND = "and"
    OR = "or"
    NOT = "not"

    # relational expressions
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"

    # arithmetic expressions
    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"
    MOD = "mod"

    # SQL operations
    PROJECT = "project"
    FILTER = "filter"
    GROUPBY = "groupby"

    # other operations
    ASSIGN = "assign"
    CONDITIONAL = "conditional"
    ALIAS = "alias"
    IS_NULL = "is_null"

    # string operation
    LENGTH = "length"
    TRIM = "trim"
    REPLACE = "replace"
    PAD = "pad"
    STRCASE = "strcase"
    STRCONTAINS = "strcontains"
    SUBSTRING = "substring"

    # operations with side effect
    INPUT = "input"


class NodeOutputType(str, Enum):
    """
    Query graph node output type
    """

    FRAME = "frame"
    SERIES = "series"
