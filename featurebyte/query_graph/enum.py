"""
This module contains all the enums used for query graph.
"""

from typing import Set

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
    COS = "cos"
    SIN = "sin"
    TAN = "tan"
    ACOS = "acos"
    ASIN = "asin"
    ATAN = "atan"

    # SQL operations
    PROJECT = "project"
    FILTER = "filter"
    GROUPBY = "groupby"
    ITEM_GROUPBY = "item_groupby"
    AGGREGATE_AS_AT = "aggregate_as_at"
    LOOKUP = "lookup"
    LOOKUP_TARGET = "lookup_target"
    JOIN = "join"
    JOIN_FEATURE = "join_feature"
    TRACK_CHANGES = "track_changes"
    FORWARD_AGGREGATE = "forward_aggregate"
    FORWARD_AGGREGATE_AS_AT = "forward_aggregate_as_at"

    # other operations
    ASSIGN = "assign"
    CONDITIONAL = "conditional"
    ALIAS = "alias"
    IS_NULL = "is_null"
    CAST = "cast"
    IS_IN = "is_in"
    IS_STRING = "is_string"  # SQL generation has not been implemented
    GET_VALUE = "get_value"

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
    DICTIONARY_KEYS = "dictionary_keys"
    GET_RANK = "get_rank"
    GET_RELATIVE_FREQUENCY = "get_relative_frequency"

    # vector related operations
    VECTOR_COSINE_SIMILARITY = "vector_cosine_similarity"

    # window functions related operations
    LAG = "lag"

    # distance operations
    HAVERSINE = "haversine"

    # operations with side effect
    INPUT = "input"
    REQUEST_COLUMN = "request_column"

    # graph node to support nested graph
    GRAPH = "graph"
    PROXY_INPUT = "proxy_input"

    # generic function node
    GENERIC_FUNCTION = "generic_function"


class NodeOutputType(StrEnum):
    """
    Query graph node output type
    """

    FRAME = "frame"
    SERIES = "series"


class GraphNodeType(StrEnum):
    """
    GraphNodeType enum is used to tag the purpose of the graph node.
    """

    # graph contains critical data info cleaning operations
    CLEANING = "cleaning"

    # graph contains view specific operations
    EVENT_VIEW = "event_view"
    ITEM_VIEW = "item_view"
    DIMENSION_VIEW = "dimension_view"
    SCD_VIEW = "scd_view"
    CHANGE_VIEW = "change_view"

    # graph node type used for offline store ingest query graph
    OFFLINE_STORE_INGEST_QUERY = "offline_store_ingest_query"

    @classmethod
    def view_graph_node_types(cls) -> Set["GraphNodeType"]:
        """
        Returns all the view graph node types

        Returns
        -------
        Set[GraphNodeType]
        """
        return {
            cls.EVENT_VIEW,
            cls.ITEM_VIEW,
            cls.DIMENSION_VIEW,
            cls.SCD_VIEW,
            cls.CHANGE_VIEW,
        }


# copy the constant here to avoid importing feast module
# feast.online_response.TIMESTAMP_POSTFIX = "__ts" (from feast/online_response.py)
# this is used to retrieve offline feature table event timestamp
FEAST_TIMESTAMP_POSTFIX = "__ts"
