"""
Module for unary operations sql generation
"""
from __future__ import annotations

from typing import Any, cast

from dataclasses import dataclass

from sqlglot import Expression, expressions

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.count_dict import CountDictTransformNode
from featurebyte.query_graph.sql.ast.generic import CastNode, IsNullNode, LagNode
from featurebyte.query_graph.sql.ast.string import (
    PadNode,
    ReplaceNode,
    StringCaseNode,
    StringContains,
    SubStringNode,
    TrimNode,
)

SUPPORTED_EXPRESSION_NODE_TYPES = [
    NodeType.IS_NULL,
    NodeType.TRIM,
    NodeType.REPLACE,
    NodeType.PAD,
    NodeType.STR_CASE,
    NodeType.STR_CONTAINS,
    NodeType.SUBSTRING,
    NodeType.DT_EXTRACT,
    NodeType.COUNT_DICT_TRANSFORM,
    NodeType.CAST,
    NodeType.LAG,
]


@dataclass
class UnaryOp(ExpressionNode):
    """Typical unary operation node (can be handled identically given the correct sqlglot
    expression)
    """

    expr: ExpressionNode
    operation: type[expressions.Expression]

    node_type_to_expression_cls = {
        NodeType.SQRT: expressions.Sqrt,
        NodeType.ABS: expressions.Abs,
        NodeType.FLOOR: expressions.Floor,
        NodeType.CEIL: expressions.Ceil,
        NodeType.NOT: expressions.Not,
        NodeType.LENGTH: expressions.Length,
        NodeType.LOG: expressions.Ln,
        NodeType.EXP: expressions.Exp,
    }
    query_node_type = list(node_type_to_expression_cls.keys())

    @property
    def sql(self) -> Expression:
        return self.operation(this=self.expr.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> UnaryOp:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        expr_cls = cls.node_type_to_expression_cls[context.query_node.type]
        node = UnaryOp(table_node=table_node, expr=input_expr_node, operation=expr_cls)
        return node


def make_expression_node(
    input_sql_nodes: list[SQLNode], node_type: NodeType, parameters: dict[str, Any]
) -> ExpressionNode:
    """Create an Expression node

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    node_type : NodeType
        Query graph node type
    parameters: dict[str, Any]
        Query node parameters

    Returns
    -------
    ExpressionNode

    Raises
    ------
    NotImplementedError
        if the query graph node type is not supported
    """
    # pylint: disable=too-many-branches
    input_expr_node = input_sql_nodes[0]
    assert isinstance(input_expr_node, ExpressionNode)
    table_node = input_expr_node.table_node
    sql_node: ExpressionNode

    if node_type == NodeType.IS_NULL:
        sql_node = IsNullNode(table_node=table_node, expr=input_expr_node)
    elif node_type == NodeType.STR_CASE:
        sql_node = StringCaseNode(
            table_node=table_node,
            expr=input_expr_node,
            case=parameters["case"],
        )
    elif node_type == NodeType.TRIM:
        sql_node = TrimNode(
            table_node=table_node,
            expr=input_expr_node,
            character=parameters["character"],
            side=parameters["side"],
        )
    elif node_type == NodeType.REPLACE:
        sql_node = ReplaceNode(
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            replacement=parameters["replacement"],
        )
    elif node_type == NodeType.PAD:
        sql_node = PadNode(
            table_node=table_node,
            expr=input_expr_node,
            side=parameters["side"],
            length=parameters["length"],
            pad=parameters["pad"],
        )
    elif node_type == NodeType.STR_CONTAINS:
        sql_node = StringContains(
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            case=parameters["case"],
        )
    elif node_type == NodeType.SUBSTRING:
        sql_node = SubStringNode(
            table_node=table_node,
            expr=input_expr_node,
            start=parameters["start"],
            length=parameters["length"],
        )
    elif node_type == NodeType.COUNT_DICT_TRANSFORM:
        sql_node = CountDictTransformNode(
            table_node=table_node,
            expr=input_expr_node,
            transform_type=parameters["transform_type"],
            include_missing=parameters.get("include_missing", True),
        )
    elif node_type == NodeType.CAST:
        sql_node = CastNode(
            table_node=table_node,
            expr=input_expr_node,
            new_type=parameters["type"],
            from_dtype=parameters["from_dtype"],
        )
    elif node_type == NodeType.LAG:
        sql_node = LagNode(
            table_node=table_node,
            expr=input_expr_node,
            entity_columns=parameters["entity_columns"],
            timestamp_column=parameters["timestamp_column"],
            offset=parameters["offset"],
        )
    else:
        raise NotImplementedError(f"Unexpected node type: {node_type}")
    return sql_node
