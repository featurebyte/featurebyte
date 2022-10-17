"""
Module for helper classes to generate engine specific SQL expressions
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Literal, Optional

from abc import abstractmethod

from sqlglot import Expression, expressions

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.literal import make_literal_value


class BaseAdapter:
    """
    Helper class to generate engine specific SQL expressions
    """

    @classmethod
    @abstractmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        """
        Expression to convert a timestamp to epoch second

        Parameters
        ----------
        timestamp_expr : Expression
            Input expression

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        """
        Expression to trim leading and / or trailing characters from string

        Parameters
        ----------
        expr : Expression
            Expression of the string input to be manipulated
        character : Optional[str]
            Character to trim, default is whitespace
        side : Literal["left", "right", "both"]
            The side of the string to be trimmed

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        """
        Expression to adjust day of week to have consistent result as pandas

        Parameters
        ----------
        extracted_expr : Expression
            Expression representing day of week calculated by the EXTRACT function

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        """
        Expression to perform DATEADD using microsecond as the time unit

        Parameters
        ----------
        quantity_expr : Expression
            Number of microseconds to add to the timestamp
        timestamp_expr : Expression
            Expression for the timestamp

        Returns
        -------
        Expression
        """


class SnowflakeAdapter(BaseAdapter):
    """
    Helper class to generate Snowflake specific SQL expressions
    """

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(
            this="DATE_PART",
            expressions=[expressions.Identifier(this="EPOCH_SECOND"), timestamp_expr],
        )

    @classmethod
    def trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        expression_class = {
            "left": fb_expressions.LTrim,
            "right": fb_expressions.RTrim,
            "both": fb_expressions.Trim,
        }[side]
        if character:
            return expression_class(this=expr, character=make_literal_value(character))
        return expression_class(this=expr)

    @classmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        # pandas: Monday=0, Sunday=6; snowflake: Sunday=0, Saturday=6
        # to follow pandas behavior, add 6 then modulo 7 to perform left-shift
        return expressions.Mod(
            this=expressions.Paren(
                this=expressions.Add(this=extracted_expr, expression=make_literal_value(6))
            ),
            expression=make_literal_value(7),
        )

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        output_expr = expressions.Anonymous(
            this="DATEADD", expressions=["microsecond", quantity_expr, timestamp_expr]
        )
        return output_expr


class DatabricksAdapter(BaseAdapter):
    """
    Helper class to generate Databricks specific SQL expressions
    """

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(this="UNIX_TIMESTAMP", expressions=[timestamp_expr])

    @classmethod
    def trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        if character is None:
            character = " "
        character_literal = make_literal_value(character)

        def _make_ltrim_expr(ex: Expression) -> Expression:
            return expressions.Anonymous(this="LTRIM", expressions=[character_literal, ex])

        def _make_rtrim_expr(ex: Expression) -> Expression:
            return expressions.Anonymous(this="RTRIM", expressions=[character_literal, ex])

        if side == "left":
            out = _make_ltrim_expr(expr)
        elif side == "right":
            out = _make_rtrim_expr(expr)
        else:
            out = _make_ltrim_expr(_make_rtrim_expr(expr))
        return out

    @classmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        # pandas: Monday=0, Sunday=6; databricks: Sunday=1, Saturday=7
        # Conversion formula: (databricks_dayofweek - 1 + 6) % 7
        return expressions.Mod(
            this=expressions.Paren(
                this=expressions.Add(this=extracted_expr, expression=make_literal_value(5))
            ),
            expression=make_literal_value(7),
        )

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        # In theory, simply DATEADD(microsecond, quantity_expr, timestamp_expr) should work.
        # However, in Databricks the quantity_expr has INT type and hence is prone to overflow
        # especially when the working unit is microsecond - overflow occurs when the quantity is
        # more than just 35 minutes (2147483647 microseconds is about 35.7 minutes)!
        #
        # To overcome that issue, this performs DATEADD in two operations - the first using minute
        # as the unit, and the second using microsecond as the unit for the remainder.
        num_microsecond_per_minute = make_literal_value(1e6 * 60)
        minute_quantity = expressions.Div(this=quantity_expr, expression=num_microsecond_per_minute)
        microsecond_quantity = expressions.Mod(
            this=quantity_expr, expression=num_microsecond_per_minute
        )
        output_expr = expressions.Anonymous(
            this="DATEADD", expressions=["minute", minute_quantity, timestamp_expr]
        )
        output_expr = expressions.Anonymous(
            this="DATEADD", expressions=["microsecond", microsecond_quantity, output_expr]
        )
        return output_expr


def get_sql_adapter(source_type: SourceType) -> BaseAdapter:
    """
    Factory that returns an engine specific adapter given source type

    Parameters
    ----------
    source_type : SourceType
        Source type information

    Returns
    -------
    BaseAdapter
        Instance of BaseAdapter
    """
    if source_type == SourceType.DATABRICKS:
        return DatabricksAdapter()
    return SnowflakeAdapter()
