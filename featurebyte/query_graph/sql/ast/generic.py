"""
Module for generic operations sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value


@dataclass
class StrExpressionNode(ExpressionNode):
    """Expression node created from string"""

    expr: str

    @property
    def sql(self) -> Expression:
        return parse_one(self.expr)


@dataclass
class ParsedExpressionNode(ExpressionNode):
    """Expression node"""

    expr: Expression

    @property
    def sql(self) -> Expression:
        return self.expr

    @classmethod
    def from_expression_node(cls, node: ExpressionNode, expr: Expression) -> ParsedExpressionNode:
        """
        Create a ParsedExpressionNode from an existing ExpressionNode

        Parameters
        ----------
        node : ExpressionNode
            The existing ExpressionNode
        expr : Expression
            The expression to use

        Returns
        -------
        ParsedExpressionNode
        """
        return ParsedExpressionNode(
            context=node.context,
            table_node=node.table_node,
            expr=expr,
        )


@dataclass
class Project(ExpressionNode):
    """Project node for a single column"""

    column_name: str

    @property
    def sql(self) -> Expression:
        assert self.table_node is not None
        return self.table_node.get_column_expr(self.column_name)

    @property
    def sql_standalone(self) -> Expression:
        assert self.table_node is not None
        return self.table_node.get_sql_for_expressions(exprs=[self.sql], aliases=[self.column_name])


@dataclass
class AliasNode(ExpressionNode):
    """Alias node that represents assignment to FeatureGroup"""

    name: str
    expr_node: ExpressionNode
    query_node_type = NodeType.ALIAS

    @property
    def sql(self) -> Expression:
        return self.expr_node.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> AliasNode:
        expr_node = context.input_sql_nodes[0]
        assert isinstance(expr_node, ExpressionNode)
        sql_node = AliasNode(
            context=context,
            table_node=expr_node.table_node,
            name=context.parameters["name"],
            expr_node=expr_node,
        )
        return sql_node


@dataclass
class Conditional(ExpressionNode):
    """Conditional node"""

    series_node: ExpressionNode
    mask: ExpressionNode
    value: Optional[Any]
    value_series: Optional[ExpressionNode]
    query_node_type = NodeType.CONDITIONAL

    @property
    def sql(self) -> Expression:
        if self.value_series is not None:
            mask_expression = self.value_series.sql
        else:
            mask_expression = make_literal_value(self.value)
        if_expr = expressions.If(this=self.mask.sql, true=mask_expression)
        expr = expressions.Case(ifs=[if_expr], default=self.series_node.sql)
        return expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Conditional:
        input_sql_nodes = context.input_sql_nodes
        assert len(input_sql_nodes) >= 2

        series_node = input_sql_nodes[0]
        mask = input_sql_nodes[1]
        value_node = None
        if len(input_sql_nodes) == 3:
            value_node = input_sql_nodes[2]
            assert isinstance(value_node, ExpressionNode)
        assert isinstance(series_node, ExpressionNode)
        assert isinstance(mask, ExpressionNode)
        input_table_node = series_node.table_node

        # Check to see if both values are present. If so, error since we don't know which one to use.
        context_value = context.parameters["value"]
        if len(input_sql_nodes) == 3 and context_value is not None:
            raise ValueError("too many values provided.")

        sql_node = Conditional(
            context=context,
            table_node=input_table_node,
            series_node=series_node,
            mask=mask,
            value=context_value,
            value_series=value_node,  # type: ignore[arg-type]
        )
        return sql_node


def make_project_node(context: SQLNodeContext) -> Project | TableNode:
    """Create a Project or ProjectMulti node

    Parameters
    ----------
    context : SQLNodeContext
        Information required to build SQLNode

    Returns
    -------
    Project | TableNode
        The appropriate SQL node for projection
    """
    table_node = context.input_sql_nodes[0]
    assert isinstance(table_node, TableNode)
    columns = context.parameters["columns"]
    sql_node: Project | TableNode
    if context.query_node.output_type == NodeOutputType.SERIES:
        sql_node = Project(context=context, table_node=table_node, column_name=columns[0])
    else:
        sql_node = table_node.subset_columns(context, columns)
    return sql_node


def make_assign_node(context: SQLNodeContext) -> TableNode:
    """
    Create a TableNode for an assign operation

    Parameters
    ----------
    context : SQLNodeContext
        Information required to build SQLNode

    Returns
    -------
    TableNode
    """
    input_sql_nodes = context.input_sql_nodes
    parameters = context.parameters
    input_table_node = input_sql_nodes[0]
    assert isinstance(input_table_node, TableNode)
    if len(input_sql_nodes) == 2:
        expr_node = input_sql_nodes[1]
    else:
        expr_node = ParsedExpressionNode(
            context, input_table_node, make_literal_value(parameters["value"])
        )
    assert isinstance(expr_node, ExpressionNode)
    sql_node = input_table_node.copy()
    sql_node.context.current_query_node = context.query_node
    sql_node.assign_column(parameters["name"], expr_node)
    return sql_node


def resolve_project_node(expr_node: ExpressionNode) -> Optional[ExpressionNode]:
    """Resolves a Project node to the original ExpressionNode due to assignment

    This is needed when we need additional information tied to original ExpressionNode than just the
    constructed sql expression. Such information is lost in a Project node.

    Parameters
    ----------
    expr_node : ExpressionNode
        The ExpressionNode to resolve if it is a Project node

    Returns
    -------
    Optional[ExpressionNode]
        The original ExpressionNode or None if the node is never assigned (e.g. original columns
        that exist in the input table)
    """
    if not isinstance(expr_node, Project):
        return expr_node
    table_node = expr_node.table_node
    assert table_node is not None
    assigned_node = table_node.get_column_node(expr_node.column_name)
    return assigned_node


def handle_filter_node(context: SQLNodeContext) -> TableNode | ExpressionNode:
    """Create a TableNode or ExpressionNode with filter condition

    Parameters
    ----------
    context : SQLNodeContext
        Information required to build SQLNode

    Returns
    -------
    TableNode | ExpressionNode
        The appropriate SQL node for the filtered result
    """
    item, mask = context.input_sql_nodes
    assert isinstance(mask, ExpressionNode)
    sql_node: TableNode | ExpressionNode
    if context.query_node.output_type == NodeOutputType.FRAME:
        assert isinstance(item, TableNode)
        sql_node = item.subset_rows(context, mask.sql)
    else:
        assert isinstance(item, ExpressionNode)
        assert isinstance(item.table_node, TableNode)
        input_table_copy = item.table_node.subset_rows(context, mask.sql)
        sql_node = ParsedExpressionNode(context, input_table_copy, item.sql)
    return sql_node
