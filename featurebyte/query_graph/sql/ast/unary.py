"""
Module for unary operations sql generation
"""
from __future__ import annotations

from typing import Any, Literal, cast

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.count_dict import CountDictTransformNode
from featurebyte.query_graph.sql.ast.string import (
    PadNode,
    ReplaceNode,
    StringCaseNode,
    StringContains,
    SubStringNode,
    TrimNode,
)
from featurebyte.query_graph.sql.ast.util import prepare_unary_input_nodes

SUPPORTED_EXPRESSION_NODE_TYPES = [
    NodeType.TRIM,
    NodeType.REPLACE,
    NodeType.PAD,
    NodeType.STR_CASE,
    NodeType.STR_CONTAINS,
    NodeType.SUBSTRING,
    NodeType.DT_EXTRACT,
    NodeType.COUNT_DICT_TRANSFORM,
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


@dataclass
class IsNullNode(ExpressionNode):
    """Node for IS_NULL operation"""

    expr: ExpressionNode
    query_node_type = NodeType.IS_NULL

    @property
    def sql(self) -> Expression:
        return expressions.Is(this=self.expr.sql, expression=expressions.Null())

    @classmethod
    def build(cls, context: SQLNodeContext) -> IsNullNode:
        table_node, expr_node = prepare_unary_input_nodes(context.input_sql_nodes)
        return IsNullNode(table_node=table_node, expr=expr_node)


@dataclass
class CastNode(ExpressionNode):
    """Node for casting operation"""

    expr: ExpressionNode
    new_type: Literal["int", "float", "str"]
    from_dtype: DBVarType
    query_node_type = NodeType.CAST

    @property
    def sql(self) -> Expression:
        if self.from_dtype == DBVarType.FLOAT and self.new_type == "int":
            # Casting to INTEGER performs rounding (could be up or down). Hence, apply FLOOR first
            # to mimic pandas astype(int)
            expr = expressions.Floor(this=self.expr.sql)
        elif self.from_dtype == DBVarType.BOOL and self.new_type == "float":
            # Casting to FLOAT from BOOL directly is not allowed
            expr = expressions.Cast(this=self.expr.sql, to=parse_one("INTEGER"))
        else:
            expr = self.expr.sql
        type_expr = {
            "int": parse_one("INTEGER"),
            "float": parse_one("FLOAT"),
            "str": parse_one("VARCHAR"),
        }[self.new_type]
        output_expr = expressions.Cast(this=expr, to=type_expr)
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> CastNode:
        table_node, input_expr_node = prepare_unary_input_nodes(context.input_sql_nodes)
        sql_node = CastNode(
            table_node=table_node,
            expr=input_expr_node,
            new_type=context.parameters["type"],
            from_dtype=context.parameters["from_dtype"],
        )
        return sql_node


@dataclass
class LagNode(ExpressionNode):
    """Node for lag operation"""

    expr: ExpressionNode
    entity_columns: list[str]
    timestamp_column: str
    offset: int
    query_node_type = NodeType.LAG

    @property
    def sql(self) -> Expression:
        partition_by = [
            expressions.Column(this=expressions.Identifier(this=col, quoted=True))
            for col in self.entity_columns
        ]
        order = expressions.Order(
            expressions=[
                expressions.Ordered(
                    this=expressions.Identifier(this=self.timestamp_column, quoted=True)
                )
            ]
        )
        output_expr = expressions.Window(
            this=expressions.Anonymous(
                this="LAG", expressions=[self.expr.sql, make_literal_value(self.offset)]
            ),
            partition_by=partition_by,
            order=order,
        )
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> LagNode:
        table_node, input_expr_node = prepare_unary_input_nodes(context.input_sql_nodes)
        parameters = context.parameters
        sql_node = LagNode(
            table_node=table_node,
            expr=input_expr_node,
            entity_columns=parameters["entity_columns"],
            timestamp_column=parameters["timestamp_column"],
            offset=parameters["offset"],
        )
        return sql_node


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

    if node_type == NodeType.STR_CASE:
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
    else:
        raise NotImplementedError(f"Unexpected node type: {node_type}")
    return sql_node
