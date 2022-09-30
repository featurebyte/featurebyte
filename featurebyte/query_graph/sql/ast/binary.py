"""
Module for binary operations sql generation
"""
from __future__ import annotations

from typing import Any

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNode, make_literal_value
from featurebyte.query_graph.sql.ast.datetime import DateAddNode, DateDiffNode, make_date_add_node
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode


@dataclass
class BinaryOp(ExpressionNode):
    """Binary operation node"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    operation: type[expressions.Expression]

    @property
    def sql(self) -> Expression:
        right_expr = self.right_node.sql
        if self.operation in {expressions.Div, expressions.Mod}:
            # Make 0 divisor null to prevent division-by-zero error
            right_expr = parse_one(f"NULLIF({right_expr.sql()}, 0)")
        if self.operation == fb_expressions.Concat:
            op_expr = self.operation(expressions=[self.left_node.sql, right_expr])
        elif self.operation == expressions.Pow:
            op_expr = self.operation(this=self.left_node.sql, power=right_expr)
        else:
            op_expr = self.operation(this=self.left_node.sql, expression=right_expr)
        return expressions.Paren(this=op_expr)


def make_binary_operation_node(
    node_type: NodeType,
    input_sql_nodes: list[SQLNode],
    parameters: dict[str, Any],
) -> BinaryOp | DateDiffNode | DateAddNode:
    """Create a BinaryOp node for eligible query node types

    Parameters
    ----------
    node_type : NodeType
        Node type
    input_sql_nodes : List[SQLNode]
        List of input SQL nodes
    parameters : dict
        Query node parameters

    Returns
    -------
    BinaryOp | DateDiffNode | DateAddNode

    Raises
    ------
    NotImplementedError
        For incompatible node types
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

    node_type_to_expression_cls = {
        # Arithmetic
        NodeType.ADD: expressions.Add,
        NodeType.SUB: expressions.Sub,
        NodeType.MUL: expressions.Mul,
        NodeType.DIV: expressions.Div,
        NodeType.MOD: expressions.Mod,
        # Relational
        NodeType.EQ: expressions.EQ,
        NodeType.NE: expressions.NEQ,
        NodeType.LT: expressions.LT,
        NodeType.LE: expressions.LTE,
        NodeType.GT: expressions.GT,
        NodeType.GE: expressions.GTE,
        # Logical
        NodeType.AND: expressions.And,
        NodeType.OR: expressions.Or,
        # String
        NodeType.CONCAT: fb_expressions.Concat,
        NodeType.COSINE_SIMILARITY: fb_expressions.CosineSim,
        NodeType.POWER: expressions.Pow,
    }

    output_node: BinaryOp | DateDiffNode | DateAddNode
    if node_type in node_type_to_expression_cls:
        expression_cls = node_type_to_expression_cls[node_type]
        output_node = BinaryOp(
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
            operation=expression_cls,
        )
    elif node_type == NodeType.DATE_DIFF:
        output_node = DateDiffNode(
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
        )
    elif node_type == NodeType.DATE_ADD:
        output_node = make_date_add_node(
            table_node=table_node, input_date_node=left_node, timedelta_node=right_node
        )
    else:
        raise NotImplementedError(f"{node_type} cannot be converted to binary operation")

    return output_node


BINARY_OPERATION_NODE_TYPES = {
    NodeType.ADD,
    NodeType.SUB,
    NodeType.MUL,
    NodeType.DIV,
    NodeType.MOD,
    NodeType.EQ,
    NodeType.NE,
    NodeType.LT,
    NodeType.LE,
    NodeType.GT,
    NodeType.GE,
    NodeType.AND,
    NodeType.OR,
    NodeType.CONCAT,
    NodeType.COSINE_SIMILARITY,
    NodeType.DATE_DIFF,
    NodeType.DATE_ADD,
    NodeType.POWER,
}
