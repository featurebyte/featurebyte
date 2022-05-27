"""
Series class
"""
from __future__ import annotations

from typing import Dict, Optional, Set, Type

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


class Series:
    """
    Implement Pandas Series like operations to manipulate database column
    """

    def __init__(
        self, node: Node, name: str | None, var_type: DBVarType, row_index_lineage: list[str]
    ):
        self.graph = QueryGraph()
        self.node = node
        self.name = name
        self.var_type = var_type
        self.row_index_lineage = tuple(row_index_lineage)

    def __getitem__(self, item: Series) -> Series:
        if isinstance(item, Series):
            if item.var_type != DBVarType.BOOL:
                raise TypeError("Only boolean Series filtering is supported!")
            if self.row_index_lineage != item.row_index_lineage:
                raise ValueError("Row index not aligned!")
            node = self.graph.add_operation(
                node_type=NodeType.FILTER,
                node_params={},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, item.node],
            )
            lineage = list(self.row_index_lineage)
            lineage.append(node.name)
            return Series(
                node=node, name=self.name, var_type=self.var_type, row_index_lineage=lineage
            )
        raise TypeError(f"Type {type(item)} not supported!")

    def _is_valid_assignment(self, py_type: type[int | float | str | bool]) -> bool:
        valid_assignment_map = {
            DBVarType.BOOL: {bool},
            DBVarType.CHAR: {},
            DBVarType.DATE: {},
            DBVarType.FLOAT: {int, float},
            DBVarType.INT: {int},
            DBVarType.VARCHAR: {str},
        }
        return py_type in valid_assignment_map[self.var_type]

    def __setitem__(self, key: Series, value: int | float | str | bool) -> None:
        if isinstance(key, Series) and isinstance(value, (int, float, str, bool)):
            if self.row_index_lineage != key.row_index_lineage:
                raise ValueError("Row index not aligned!")
            if key.var_type != DBVarType.BOOL:
                raise TypeError("Only boolean Series filtering is supported!")
            if not self._is_valid_assignment(type(value)):
                raise ValueError(
                    f"Key type {type(key)} with value type {type(value)} not supported!"
                )
            self.node = self.graph.add_operation(
                node_type=NodeType.COND_ASSIGN,
                node_params={"value": value},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, key.node],
            )
        else:
            raise TypeError(f"Key type {type(key)} with value type {type(value)} not supported!")

    def _binary_op(
        self,
        other: int | float | str | bool | Series,
        node_type: NodeType,
        output_var_type: DBVarType,
    ) -> Series:
        lineage = list(self.row_index_lineage)
        if isinstance(other, (int, float, str, bool)):
            node = self.graph.add_operation(
                node_type=node_type,
                node_params={"value": other},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node],
            )
            return Series(node=node, name=None, var_type=output_var_type, row_index_lineage=lineage)
        if isinstance(other, Series):
            node = self.graph.add_operation(
                node_type=node_type,
                node_params={},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, other.node],
            )
            return Series(node=node, name=None, var_type=output_var_type, row_index_lineage=lineage)

    def _logical_binary_op(self, other: bool | Series, node_type: NodeType) -> Series:
        if self.var_type == DBVarType.BOOL:
            if isinstance(other, bool) or (
                isinstance(other, Series) and other.var_type == DBVarType.BOOL
            ):
                return self._binary_op(
                    other=other, node_type=node_type, output_var_type=DBVarType.BOOL
                )

        other_type = f"{type(other)}"
        if isinstance(other, Series):
            other_type = f"{other_type}[{other.var_type}]"
        raise TypeError(
            f"Not supported operation '{node_type}' between {self.var_type} and {other_type}!"
        )

    def __and__(self, other: bool | Series) -> Series:
        return self._logical_binary_op(other, NodeType.AND)

    def __or__(self, other: bool | Series) -> Series:
        return self._logical_binary_op(other, NodeType.OR)

    def _relational_binary_op(
        self, other: int | float | str | bool | Series, node_type: NodeType
    ) -> Series:
        if (
            (isinstance(other, Series) and other.var_type == self.var_type)
            or (self.var_type == DBVarType.INT and isinstance(other, int))
            or (self.var_type == DBVarType.FLOAT and isinstance(other, float))
            or (self.var_type == DBVarType.VARCHAR and isinstance(other, str))
            or (self.var_type == DBVarType.BOOL and isinstance(other, bool))
        ):
            return self._binary_op(other=other, node_type=node_type, output_var_type=self.var_type)

        other_type = f"{type(other)}"
        if isinstance(other, Series):
            other_type = f"{other_type}[{other.var_type}]"
        raise TypeError(
            f"Not supported operation '{node_type}' between {self.var_type} and {other_type}!"
        )

    def __eq__(self, other: object) -> bool | Series:
        if isinstance(other, (int, float, str, bool, Series)):
            return self._relational_binary_op(other, NodeType.EQ)
        return super().__eq__(other)

    def __ne__(self, other: int | float | str | bool | Series) -> bool | Series:
        if isinstance(other, (int, float, str, bool, Series)):
            return self._relational_binary_op(other, NodeType.NE)
        return super().__ne__(other)

    def __lt__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.LT)

    def __le__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.LE)

    def __gt__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.GT)

    def __ge__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.GE)
