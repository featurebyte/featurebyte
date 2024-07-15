"""
SparkAdapter class for generating Spark specific SQL expressions
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter.databricks import DatabricksAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value


class SparkAdapter(DatabricksAdapter):
    """
    Helper class to generate Spark specific SQL expressions

    Spark is the OSS version of Databricks, so it shares most of the same SQL syntax.
    """

    source_type = SourceType.SPARK

    @classmethod
    def is_qualify_clause_supported(cls) -> bool:
        """
        Spark does not support the `QUALIFY` clause though DataBricks does.

        Returns
        -------
        bool
        """
        return False

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    @classmethod
    def datediff_microsecond(
        cls, timestamp_expr_1: Expression, timestamp_expr_2: Expression
    ) -> Expression:
        def _to_microseconds(expr: Expression) -> Expression:
            return expressions.Mul(
                this=cls.to_seconds_double(expr),
                expression=make_literal_value(1e6),
            )

        return expressions.Paren(
            this=expressions.Sub(
                this=_to_microseconds(timestamp_expr_2),
                expression=_to_microseconds(timestamp_expr_1),
            )
        )

    @classmethod
    def str_contains(cls, expr: Expression, pattern: str) -> Expression:
        pattern = "%{}%".format(pattern.replace("%", "\\%"))
        return expressions.Like(this=expr, expression=make_literal_value(pattern))
