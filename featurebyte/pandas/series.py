"""
Series class
"""
from __future__ import annotations

from typing import List

from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import Node, QueryGraph


class Series:
    """
    Implement Pandas Series like operations to manipulate database column
    """

    def __init__(self, node: Node, name: str, var_type: DBVarType, row_index_lineage: list[str]):
        self.graph = QueryGraph()
        self.node = node
        self.name = name
        self.var_type = var_type
        self.row_index_lineage = tuple(row_index_lineage)
