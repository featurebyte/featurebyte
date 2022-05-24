"""
Pandas DataFrame like operations to manipulate execution graph
"""
from __future__ import annotations

from featurebyte.execution_graph.enum import NodeOutputType, NodeType
from featurebyte.execution_graph.graph import ExecutionGraph, Node


class Series:
    def __init__(self, node: Node):
        self.execution_graph = ExecutionGraph()
        self.node = node

    def _binary_op(self, other: Series, node_type: NodeType) -> Series:
        node = self.execution_graph.add_operation(
            node_type=node_type,
            node_params={},
            node_output_type=NodeOutputType.Series,
            input_nodes=[self.node, other.node],
        )
        return Series(node)

    def __and__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.AND)

    def __or__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.OR)

    def __eq__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.EQ)

    def __ne__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.NE)

    def __lt__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.LT)

    def __le__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.LE)

    def __gt__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.GT)

    def __ge__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.GE)

    def __add__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.ADD)

    def __sub__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.SUB)

    def __mul__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.MUL)

    def __truediv__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.DIV)

    def __mod__(self, other: Series) -> Series:
        return self._binary_op(other, NodeType.MOD)
