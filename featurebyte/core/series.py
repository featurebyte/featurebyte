"""
Series class
"""
from __future__ import annotations

from typing import Any

from featurebyte.core.operation import OpsMixin
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


class Series(OpsMixin):
    """
    Implement Pandas Series like operations to manipulate database column
    """

    def __init__(
        self,
        node: Node,
        name: str | None,
        var_type: DBVarType,
        lineage: tuple[str, ...],
        row_index_lineage: tuple[str, ...],
    ):
        self.graph = QueryGraph()
        self.node = node
        self.name = name
        self.var_type = var_type
        self.lineage = lineage
        self.row_index_lineage = row_index_lineage

    def __repr__(self) -> str:
        return f"Series[{self.var_type}](name={self.name}, node.name={self.node.name})"

    def __getitem__(self, item: Series) -> Series:
        if isinstance(item, Series):
            node = self._add_filter_operation(
                item=self, mask=item, node_output_type=NodeOutputType.SERIES
            )
            return Series(
                node=node,
                name=self.name,
                var_type=self.var_type,
                lineage=self._append_to_lineage(self.lineage, node.name),
                row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
            )
        raise KeyError(f"Series indexing with value '{item}' not supported!")

    @staticmethod
    def _is_assignment_valid(left_dbtype: DBVarType, right_value: Any) -> bool:
        """
        Check whether the right value python builtin type can be assigned to left value database type.
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

    def __setitem__(self, key: Series, value: int | float | str | bool) -> None:
        if isinstance(key, Series) and self._is_supported_scalar_type(value):
            if self.row_index_lineage != key.row_index_lineage:
                raise ValueError(f"Row indices between '{self}' and '{key}' are not aligned!")
            if key.var_type != DBVarType.BOOL:
                raise TypeError("Only boolean Series filtering is supported!")
            if not self._is_assignment_valid(self.var_type, value):
                raise ValueError(f"Setting key '{key}' with value '{value}' not supported!")
            self.node = self.graph.add_operation(
                node_type=NodeType.COND_ASSIGN,
                node_params={"value": value},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node, key.node],
            )
            self.lineage = self._append_to_lineage(self.lineage, self.node.name)
        else:
            raise TypeError(f"Key '{key}' not supported!")

    @staticmethod
    def _is_a_series_of_var_type(item: Any, var_type: DBVarType) -> bool:
        """
        Check whether the input item is Series type and has the specified variable type
        """
        return isinstance(item, Series) and item.var_type == var_type

    def _binary_op(
        self,
        other: int | float | str | bool | Series,
        node_type: NodeType,
        output_var_type: DBVarType,
    ) -> Series:
        """
        Apply binary operation between self & other objects
        """
        if self._is_supported_scalar_type(other):
            node = self.graph.add_operation(
                node_type=node_type,
                node_params={"value": other},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node],
            )
            return Series(
                node=node,
                name=None,
                var_type=output_var_type,
                lineage=self._append_to_lineage(self.lineage, node.name),
                row_index_lineage=self.row_index_lineage,
            )

        node = self.graph.add_operation(
            node_type=node_type,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.node, other.node],
        )
        return Series(
            node=node,
            name=None,
            var_type=output_var_type,
            lineage=self._append_to_lineage(self.lineage, node.name),
            row_index_lineage=self.row_index_lineage,
        )

    def _logical_binary_op(self, other: bool | Series, node_type: NodeType) -> Series:
        """
        Apply binary logical operation between self & other objects
        """
        if self.var_type == DBVarType.BOOL:
            if isinstance(other, bool) or self._is_a_series_of_var_type(other, DBVarType.BOOL):
                return self._binary_op(
                    other=other, node_type=node_type, output_var_type=DBVarType.BOOL
                )

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __and__(self, other: bool | Series) -> Series:
        return self._logical_binary_op(other, NodeType.AND)

    def __or__(self, other: bool | Series) -> Series:
        return self._logical_binary_op(other, NodeType.OR)

    def _relational_binary_op(
        self, other: int | float | str | bool | Series, node_type: NodeType
    ) -> Series:
        """
        Apply binary relational operation between self & other objects
        """
        is_supported_scalar_type = self.var_type in self.dbtype_pytype_map
        if self._is_a_series_of_var_type(other, self.var_type) or (
            is_supported_scalar_type
            and isinstance(other, self.dbtype_pytype_map.get(self.var_type, Series))
        ):
            return self._binary_op(other=other, node_type=node_type, output_var_type=DBVarType.BOOL)

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __eq__(self, other: int | float | str | bool | Series) -> Series:  # type: ignore
        return self._relational_binary_op(other, NodeType.EQ)

    def __ne__(self, other: int | float | str | bool | Series) -> Series:  # type: ignore
        return self._relational_binary_op(other, NodeType.NE)

    def __lt__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.LT)

    def __le__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.LE)

    def __gt__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.GT)

    def __ge__(self, other: int | float | str | bool | Series) -> Series:
        return self._relational_binary_op(other, NodeType.GE)

    def _arithmetic_binary_op(self, other: int | float | Series, node_type: NodeType) -> Series:
        """
        Apply binary arithmetic operation between self & other objects
        """
        supported_types = {DBVarType.INT, DBVarType.FLOAT}
        if self.var_type not in supported_types:
            raise TypeError(f"{self} does not support operation '{node_type}'.")
        if (isinstance(other, Series) and other.var_type in supported_types) or isinstance(
            other, (int, float)
        ):
            output_var_type = DBVarType.FLOAT
            if (
                self.var_type == DBVarType.INT
                and (isinstance(other, int) or self._is_a_series_of_var_type(other, DBVarType.INT))
                and node_type not in {NodeType.DIV}
            ):
                output_var_type = DBVarType.INT
            return self._binary_op(
                other=other, node_type=node_type, output_var_type=output_var_type
            )

        raise TypeError(f"Not supported operation '{node_type}' between '{self}' and '{other}'!")

    def __add__(self, other: int | float | Series) -> Series:
        return self._arithmetic_binary_op(other, NodeType.ADD)

    def __sub__(self, other: int | float | Series) -> Series:
        return self._arithmetic_binary_op(other, NodeType.SUB)

    def __mul__(self, other: int | float | Series) -> Series:
        return self._arithmetic_binary_op(other, NodeType.MUL)

    def __truediv__(self, other: int | float | Series) -> Series:
        return self._arithmetic_binary_op(other, NodeType.DIV)
