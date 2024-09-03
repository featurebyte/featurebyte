"""
Module for binary operations sql generation
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class BinaryOp(ExpressionNode):
    """Binary operation node"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    operation: type[expressions.Expression]

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
    query_node_type = list(node_type_to_expression_cls.keys())

    @property
    def sql(self) -> Expression:
        right_expr = self.right_node.sql
        if self.operation in {expressions.Div, expressions.Mod}:
            # Make 0 divisor null to prevent division-by-zero error
            right_expr = parse_one(f"NULLIF({right_expr.sql()}, 0)")
        if self.operation == fb_expressions.CosineSim:
            op_expr = self.context.adapter.call_udf(
                "F_COUNT_DICT_COSINE_SIMILARITY",
                [self.left_node.sql, right_expr],
            )
        elif self.operation == fb_expressions.Concat:
            op_expr = self.operation(expressions=[self.left_node.sql, right_expr])
        elif self.operation == expressions.Mod:
            op_expr = self.context.adapter.modulo(self.left_node.sql, right_expr)
        else:
            op_expr = self.operation(this=self.left_node.sql, expression=right_expr)
        return expressions.Paren(this=op_expr)

    @classmethod
    def build(cls, context: SQLNodeContext) -> BinaryOp:
        expression_cls = cls.node_type_to_expression_cls[context.query_node.type]
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        output_node = BinaryOp(
            context=context,
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
            operation=expression_cls,
        )
        return output_node
