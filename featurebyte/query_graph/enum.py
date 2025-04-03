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
    NON_TILE_WINDOW_AGGREGATE = "non_tile_window_aggregate"
    TIME_SERIES_WINDOW_AGGREGATE = "time_series_window_aggregate"

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
    ZIP_TIMESTAMP_TZ_TUPLE = "zip_timestamp_tz_tuple"
    ADD_TIMESTAMP_SCHEMA = "add_timestamp_schema"
    TO_TIMESTAMP_FROM_EPOCH = "to_timestamp_from_epoch"

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

    @classmethod
    def aggregation_and_lookup_node_types(cls) -> Set["NodeType"]:
        """
        Returns all the aggregation and lookup node types

        Returns
        -------
        Set[NodeType]
        """
        return {
            # aggregation nodes
            cls.GROUPBY,
            cls.NON_TILE_WINDOW_AGGREGATE,
            cls.TIME_SERIES_WINDOW_AGGREGATE,
            cls.FORWARD_AGGREGATE,
            # item groupby
            cls.ITEM_GROUPBY,
            # aggregation as at
            cls.AGGREGATE_AS_AT,
            cls.FORWARD_AGGREGATE_AS_AT,
            # lookup nodes
            cls.LOOKUP,
            cls.LOOKUP_TARGET,
        }

    @classmethod
    def non_aggregation_with_timestamp_node_types(cls) -> Set["NodeType"]:
        """
        Returns all the non aggregation with timestamp node types. This is used to exclude timestamp_schema from
        non-aggregation nodes for handling hashing backward compatibility.

        Returns
        -------
        Set[NodeType]
        """
        return {
            # generic.py
            cls.JOIN,
            cls.TRACK_CHANGES,
            # date.py
            cls.DT_EXTRACT,
            cls.DATE_DIFF,
            cls.DATE_ADD,
            cls.INPUT,
        }


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
    TIME_SERIES_VIEW = "time_series_view"

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
            cls.TIME_SERIES_VIEW,
        }


# copy the constant here to avoid importing feast module
# feast.online_response.TIMESTAMP_POSTFIX = "__ts" (from feast/online_response.py)
# this is used to retrieve offline feature table event timestamp
FEAST_TIMESTAMP_POSTFIX = "__ts"
