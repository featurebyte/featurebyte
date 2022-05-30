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
            raise ValueError(f"Row indices between '{item}' and '{mask}' are not aligned!")

        node = QueryGraph().add_operation(
            node_type=NodeType.FILTER,
            node_params={},
            node_output_type=node_output_type,
            input_nodes=[item.node, mask.node],
        )
        return node

    @staticmethod
    def _is_assignment_valid(left_dbtype: DBVarType, right_value: Any) -> bool:
        """
        Check whether the right value python builtin type can be assigned to left value database type.

        Parameters
        ----------
        left_dbtype: DBVarType
            database variable type
        right_value: Any
            value to be checked whether its type can be assigned to DBVarType

        Returns
        -------
        bool
            boolean value to indicate the whether the assignment is valid or not
        """
        valid_assignment_map = {
            DBVarType.BOOL: (bool,),
            DBVarType.INT: (int,),
            DBVarType.FLOAT: (int, float),
            DBVarType.CHAR: (),
            DBVarType.VARCHAR: (str,),
            DBVarType.DATE: (),
        }
        return isinstance(right_value, valid_assignment_map[left_dbtype])

    @staticmethod
    def _append_to_lineage(lineage: tuple[str, ...], node_name: str) -> tuple[str, ...]:
        """
        Add operation node name to the (row-index) lineage (list of node names)

        Parameters
        ----------
        lineage: tuple[str, ...]
            tuple of node names to represent the feature/row-index lineage
        node_name: str
            operation node name

        Returns
        -------
        updated_lineage: tuple[str, ...]
            updated lineage after adding the new operation name

        """
        output = list(lineage)
        output.append(node_name)
        return tuple(output)
