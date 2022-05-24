"""
Pandas Series like operations to manipulate execution graph
"""
from __future__ import annotations

from featurebyte.execution_graph.enum import NodeOutputType, NodeType
from featurebyte.execution_graph.graph import ExecutionGraph, Node
from featurebyte.pandas.series import Series


class DataFrame:
    def __init__(self, node: Node):
        self.execution_graph = ExecutionGraph()
        self.node = node

    @classmethod
    def from_source(cls, **kwargs):
        node = ExecutionGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params=kwargs,
            node_output_type=NodeOutputType.DataFrame,
            input_nodes=[],
        )
        return DataFrame(node)

    def __getitem__(self, item):
        if isinstance(item, str):
            node = self.execution_graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.Series,
                input_nodes=[self.node],
            )
            return Series(node)
        elif isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            node = self.execution_graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": item},
                node_output_type=NodeOutputType.DataFrame,
                input_nodes=[self.node],
            )
            return DataFrame(node)
        elif isinstance(item, Series):
            node = self.execution_graph.add_operation(
                node_type=NodeType.FILTER,
                node_params={},
                node_output_type=NodeOutputType.DataFrame,
                input_nodes=[self.node, item.node],
            )
            return DataFrame(node)
        raise TypeError

    def __setitem__(self, key, value):
        if not isinstance(key, str) or not isinstance(value, Series):
            raise TypeError

        self.node = self.execution_graph.add_operation(
            node_type=NodeType.ASSIGN,
            node_params={},
            node_output_type=NodeOutputType.DataFrame,
            input_nodes=[self.node, value.node],
        )
