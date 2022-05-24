"""
This module contains all the enums used for query graph.
"""
from enum import Enum


class NodeOutputType(str, Enum):
    """
    Query graph node output type
    """

    DataFrame = "DataFrame"
    Series = "Series"
