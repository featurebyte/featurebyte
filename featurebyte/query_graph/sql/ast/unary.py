"""
Module for unary operations sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union, cast

from sqlglot import expressions
from sqlglot.expressions import Expression
from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.util import prepare_unary_input_nodes


@dataclass
class UnaryOp(ExpressionNode):
    """Typical unary operation node (can be handled identically given the correct sqlglot
    expression)
    """

    expr: ExpressionNode
    operation: Union[type[expressions.Expression], str]

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
    node_type_to_function = {
        NodeType.COS: "COS",
        NodeType.SIN: "SIN",
        NodeType.TAN: "TAN",
        NodeType.ACOS: "ACOS",
        NodeType.ASIN: "ASIN",
        NodeType.ATAN: "ATAN",
    }
    query_node_type = list([*node_type_to_expression_cls.keys(), *node_type_to_function.keys()])

    @property
    def sql(self) -> Expression:
        if isinstance(self.operation, str):
            return expressions.Anonymous(this=self.operation, expressions=[self.expr.sql])
        return self.operation(this=self.expr.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> UnaryOp:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        node_type = context.query_node.type
        expr_cls: Union[type[expressions.Expression], str]
        if node_type in cls.node_type_to_expression_cls:
            expr_cls = cls.node_type_to_expression_cls[node_type]
        else:
            expr_cls = cls.node_type_to_function[node_type]

        node = UnaryOp(
            context=context, table_node=table_node, expr=input_expr_node, operation=expr_cls
        )
        return node


@dataclass
class IsNullNode(ExpressionNode):
    """Node for IS_NULL operation"""

    expr: ExpressionNode
    query_node_type = NodeType.IS_NULL

    @property
    def sql(self) -> Expression:
        return expressions.Paren(
            this=expressions.Is(this=self.expr.sql, expression=expressions.Null())
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> IsNullNode:
        table_node, expr_node, _ = prepare_unary_input_nodes(context)
        return IsNullNode(context=context, table_node=table_node, expr=expr_node)


@dataclass
class BaseCastNode(ExpressionNode):
    """Base class for casting operations"""

    expr: ExpressionNode
    new_type: Literal["int", "float", "str"]
    from_dtype: DBVarType

    def _get_cast_expression_class(self) -> type[expressions.Expression]:
        """
        Get the appropriate cast expression class (Cast or TryCast).

        Raises
        ------
        NotImplementedError
            If the method is not implemented in the subclass
        """
        raise NotImplementedError("Subclasses must implement _get_cast_expression_class")

    def _prepare_input_expression(self) -> Expression:
        """
        Prepare the input expression with necessary transformations.

        Returns
        -------
        Expression
            The prepared input expression
        """
        if self.from_dtype == DBVarType.FLOAT and self.new_type == "int":
            # Casting to INTEGER performs rounding (could be up or down). Hence, apply FLOOR first
            # to mimic pandas astype(int)
            return expressions.Floor(this=self.expr.sql)
        elif self.from_dtype == DBVarType.BOOL and self.new_type == "float":
            # Casting to FLOAT from BOOL directly is not allowed
            cast_class = self._get_cast_expression_class()
            return cast_class(this=self.expr.sql, to=expressions.DataType.build("BIGINT"))
        else:
            return self.expr.sql

    def _get_target_data_type(self) -> expressions.DataType:
        """
        Get the target data type for the cast operation.

        Returns
        -------
        expressions.DataType
            The target data type
        """
        type_mapping = {
            "int": "BIGINT",
            "float": "DOUBLE",
            "str": "VARCHAR",
        }
        return expressions.DataType.build(type_mapping[self.new_type])

    @property
    def sql(self) -> Expression:
        expr = self._prepare_input_expression()
        type_expr = self._get_target_data_type()
        cast_class = self._get_cast_expression_class()
        output_expr = cast_class(this=expr, to=type_expr)
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> "BaseCastNode":
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = cls(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            new_type=parameters["type"],
            from_dtype=parameters["from_dtype"],
        )
        return sql_node


@dataclass
class CastNode(BaseCastNode):
    """Node for casting operation"""

    query_node_type = NodeType.CAST

    def _get_cast_expression_class(self) -> type[expressions.Expression]:
        return expressions.Cast


@dataclass
class TryCastNode(BaseCastNode):
    """Node for try casting operation (returns NULL on cast failure instead of error)"""

    query_node_type = NodeType.TRY_CAST

    def _get_cast_expression_class(self) -> type[expressions.Expression]:
        return expressions.TryCast


@dataclass
class LagNode(ExpressionNode):
    """Node for lag operation"""

    expr: ExpressionNode
    timestamp_node: ExpressionNode
    entity_nodes: list[ExpressionNode]
    offset: int
    query_node_type = NodeType.LAG

    @property
    def sql(self) -> Expression:
        partition_by = [entity_node.sql for entity_node in self.entity_nodes]
        order = expressions.Order(expressions=[expressions.Ordered(this=self.timestamp_node.sql)])
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
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        _, *entity_nodes, timestamp_node = context.input_sql_nodes
        sql_node = LagNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            timestamp_node=cast(ExpressionNode, timestamp_node),
            entity_nodes=[cast(ExpressionNode, node) for node in entity_nodes],
            offset=parameters["offset"],
        )
        return sql_node


@dataclass
class AddTimestampSchemaNode(ExpressionNode):
    """Node for adding timestamp schema to the column"""

    expr: ExpressionNode
    query_node_type = NodeType.ADD_TIMESTAMP_SCHEMA

    @property
    def sql(self) -> Expression:
        return self.expr.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> AddTimestampSchemaNode:
        table_node, expr_node, _ = prepare_unary_input_nodes(context)
        return AddTimestampSchemaNode(context=context, table_node=table_node, expr=expr_node)
