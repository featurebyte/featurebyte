"""
Module for constructs to work with template SQL code with placeholders
"""

from __future__ import annotations

from typing import Any, Optional

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import sql_to_string


class SqlExpressionTemplate:
    """
    Representation of a SQL template with placeholders

    Parameters
    ----------
    sql_expr : Expression
        SQL expression with placeholders in the syntax tree
    source_type : SourceType
        Source type information
    """

    def __init__(self, sql_expr: Expression, source_type: Optional[SourceType] = None):
        self.sql_expr = sql_expr
        self.source_type = source_type

    def render(self, data: dict[str, Any] | None = None, as_str: bool = True) -> str | Expression:
        """
        Render the template by replacing placeholders with actual values

        Parameters
        ----------
        data : dict[str, Any]
            Mapping from placeholder names to replacement values
        as_str : bool
            Whether to return the output as an Expression or convert it to string

        Returns
        -------
        str | Expression
        """
        if data is None:
            data = {}
        rendered_expr = self.sql_expr
        for placeholder_name, replacement_value in data.items():
            rendered_expr = rendered_expr.transform(
                lambda node: self._replace_placeholder(
                    node,
                    placeholder_name,
                    replacement_value,
                )
            )
        if as_str:
            assert self.source_type is not None
            return sql_to_string(rendered_expr, source_type=self.source_type)
        return rendered_expr

    @classmethod
    def _replace_placeholder(
        cls, node: expressions.Expression, placeholder_name: str, value: Any
    ) -> Expression:
        if isinstance(node, expressions.Identifier) and node.this == placeholder_name:
            if isinstance(value, expressions.Expression):
                return value
            return make_literal_value(value)
        return node
