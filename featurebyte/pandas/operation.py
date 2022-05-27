"""
OpsMixin class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph

if TYPE_CHECKING:
    from featurebyte.pandas.frame import DataFrame
    from featurebyte.pandas.series import Series


class OpsMixin:
    """
    OpsMixin contains common properties & operations shared between DataFrame & Series
    """

    @property
    def pytype_dbtype_map(self):
        """
        Supported python builtin scalar type to database type mapping
        """
        return {
            bool: DBVarType.BOOL,
            int: DBVarType.INT,
            float: DBVarType.FLOAT,
            str: DBVarType.VARCHAR,
        }

    @property
    def dbtype_pytype_map(self):
        """
        Supported database type mapping to python builtin scalar type
        """
        return {val: key for key, val in self.pytype_dbtype_map}

    @staticmethod
    def _add_filter_operation(
        item: DataFrame | Series, mask: Series, node_output_type: NodeOutputType
    ) -> Node:
        """
        Add filter node into the graph & return the node
        """
        if mask.var_type != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")
        if item.row_index_lineage != mask.row_index_lineage:
            raise ValueError("Row index not aligned!")

        node = QueryGraph().add_operation(
            node_type=NodeType.FILTER,
            node_params={},
            node_output_type=node_output_type,
            input_nodes=[item.node, mask.node],
        )
        return node
