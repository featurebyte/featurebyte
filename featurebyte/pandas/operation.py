"""
OpsMixin class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    def pytype_dbtype_map(self) -> dict[Any, Any]:
        """
        Supported python builtin scalar type to database type mapping

        Returns
        -------
        dict
            Mapping from supported builtin type to DB type
        """
        return {
            bool: DBVarType.BOOL,
            int: DBVarType.INT,
            float: DBVarType.FLOAT,
            str: DBVarType.VARCHAR,
        }

    @property
    def dbtype_pytype_map(self) -> dict[Any, Any]:
        """
        Supported database type mapping to python builtin scalar type

        Returns
        -------
        dict
            Mapping from DB type to supported builtin type
        """
        return {val: key for key, val in self.pytype_dbtype_map.items()}

    @staticmethod
    def _add_filter_operation(
        item: DataFrame | Series, mask: Series, node_output_type: NodeOutputType
    ) -> Node:
        """
        Add filter node into the graph & return the node

        Parameters
        ----------
        item: DataFrame | Series
            object to be filtered
        mask: Series
            mask used to filter the item object
        node_output_type: NodeOutputType
            note output type

        Returns
        -------
        Node
            Filter node

        Raises
        ------
        TypeError
            if mask Series is not boolean type
        ValueError
            if the row index between item object & mask are not aligned
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
