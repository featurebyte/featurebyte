"""
This module contains all the enums used for query graph.
"""
from featurebyte.enum import StrEnum


class NodeType(StrEnum):
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

    # numerical functions
    ABS = "abs"
    POWER = "power"
    SQRT = "sqrt"
    FLOOR = "floor"
    CEIL = "ceil"
    LOG = "log"
    EXP = "exp"

    # SQL operations
    PROJECT = "project"
    FILTER = "filter"
    GROUPBY = "groupby"

    # other operations
    ASSIGN = "assign"
    CONDITIONAL = "conditional"
    ALIAS = "alias"
    IS_NULL = "is_null"
    CAST = "cast"

    # string operations
    LENGTH = "length"
    TRIM = "trim"
    REPLACE = "replace"
    PAD = "pad"
    STR_CASE = "str_case"
    STR_CONTAINS = "str_contains"
    SUBSTRING = "substring"
    CONCAT = "concat"

    # datetime related operations
    DT_EXTRACT = "dt_extract"
    TIMEDELTA_EXTRACT = "timedelta_extract"
    DATE_DIFF = "date_diff"
    TIMEDELTA = "timedelta"
    DATE_ADD = "date_add"

    # count dict related operations
    COUNT_DICT_TRANSFORM = "count_dict_transform"
    COSINE_SIMILARITY = "cosine_similarity"

    # window functions related operations
    LAG = "lag"

    # operations with side effect
    INPUT = "input"


class NodeOutputType(StrEnum):
    """
    Query graph node output type
    """

    FRAME = "frame"
    SERIES = "series"
