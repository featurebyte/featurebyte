"""
Utilities for building SQLNode
"""
from __future__ import annotations

from typing import Any

from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value


def prepare_binary_op_input_nodes(
    context: SQLNodeContext,
) -> tuple[TableNode, ExpressionNode, ExpressionNode]:
    """
    Perform common preparation on binary ops input nodes, such as constructing literal value
    expression and swapping left right operands when applicable

    Parameters
    ----------
    context: SQLNodeContext
        Information related to SQL node building

    Returns
    -------
    tuple[TableNode, ExpressionNode, ExpressionNode]
    """
    input_sql_nodes = context.input_sql_nodes
    parameters = context.parameters
    left_node = input_sql_nodes[0]
    assert isinstance(left_node, ExpressionNode)
    table_node = left_node.table_node
    right_node: Any
    if len(input_sql_nodes) == 1:
        # When the other value is a scalar
        literal_value = make_literal_value(parameters["value"])
        right_node = ParsedExpressionNode(
            context=context, table_node=table_node, expr=literal_value
        )
    else:
        # When the other value is a Series
        right_node = input_sql_nodes[1]

    if isinstance(right_node, ExpressionNode) and parameters.get("right_op"):
        # Swap left & right objects if the operation from the right object
        left_node, right_node = right_node, left_node

    if table_node is None:
        # In this case, the left node is a column from the request data. The right node must be from
        # a feature and has a valid table node.
        assert right_node.table_node is not None
        table_node = right_node.table_node

    return table_node, left_node, right_node


def prepare_unary_input_nodes(
    context: SQLNodeContext,
) -> tuple[TableNode, ExpressionNode, dict[str, Any]]:
    """Extract TableNode and ExpressionNode in a unary operation

    Parameters
    ----------
    context: SQLNodeContext
        Information related to SQL node building

    Returns
    -------
    tuple[TableNode, ExpressionNode, dict[str, Any]]
    """
    input_expr_node = context.input_sql_nodes[0]
    assert isinstance(input_expr_node, ExpressionNode)
    table_node = input_expr_node.table_node
    assert table_node is not None
    return table_node, input_expr_node, context.parameters
