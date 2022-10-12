"""
Utilities for building SQLNode
"""
from __future__ import annotations

from typing import Any

from featurebyte.query_graph.sql.ast.base import (
    ExpressionNode,
    SQLNode,
    TableNode,
    make_literal_value,
)
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode


def prepare_binary_op_input_nodes(
    input_sql_nodes: list[SQLNode], parameters: dict[str, Any]
) -> tuple[TableNode, ExpressionNode, ExpressionNode]:
    """
    Perform common preparation on binary ops input nodes, such as constructing literal value
    expression and swapping left right operands when applicable

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    parameters : dict[str, Any]
        Query graph node parameters

    Returns
    -------
    tuple[TableNode, ExpressionNode, ExpressionNode]
    """

    left_node = input_sql_nodes[0]
    assert isinstance(left_node, ExpressionNode)
    table_node = left_node.table_node
    right_node: Any
    if len(input_sql_nodes) == 1:
        # When the other value is a scalar
        literal_value = make_literal_value(parameters["value"])
        right_node = ParsedExpressionNode(table_node=table_node, expr=literal_value)
    else:
        # When the other value is a Series
        right_node = input_sql_nodes[1]

    if isinstance(right_node, ExpressionNode) and parameters.get("right_op"):
        # Swap left & right objects if the operation from the right object
        left_node, right_node = right_node, left_node

    return table_node, left_node, right_node
