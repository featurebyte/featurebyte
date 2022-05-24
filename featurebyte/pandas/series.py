"""
Pandas DataFrame like operations to manipulate execution graph
"""
from __future__ import annotations

from typing import Set

from featurebyte.execution_graph.enum import NodeOutputType, NodeType
from featurebyte.execution_graph.graph import ExecutionGraph, Node


class Series:
    def __init__(self, node: Node):
        self.execution_graph = ExecutionGraph()
        self.node = node

    def _binary_op(self, other: Series, node_type: NodeType, support_types: set) -> Series:
        if isinstance(other, Series) and Series in support_types:
            node = self.execution_graph.add_operation(
                node_type=node_type,
                node_params={},
                node_output_type=NodeOutputType.Series,
                input_nodes=[self.node, other.node],
            )
            return Series(node)
        elif (
            isinstance(other, (int, float, bool))
            and support_types.intersection({int, float, bool})
            and type(other) in support_types
        ):
            node = self.execution_graph.add_operation(
                node_type=node_type,
                node_params={"value": other},
                node_output_type=NodeOutputType.Series,
                input_nodes=[self.node],
            )
            return Series(node)
        raise TypeError

    def __and__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.AND, {Series, bool})

    def __or__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.OR, {Series, bool})

    def __eq__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.EQ, {Series})

    def __ne__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.NE, {Series})

    def __lt__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.LT, {Series})

    def __le__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.LE, {Series})

    def __gt__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.GT, {Series})

    def __ge__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.GE, {Series})

    def __add__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.ADD, {Series})

    def __sub__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.SUB, {Series})

    def __mul__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.MUL, {Series})

    def __truediv__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.DIV, {Series})

    def __mod__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.MOD, {Series})
