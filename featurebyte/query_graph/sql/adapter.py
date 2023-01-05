"""
Module for helper classes to generate engine specific SQL expressions
"""
from __future__ import annotations

from typing import Literal, Optional

from abc import abstractmethod

from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.enum import DBVarType, SourceType, StrEnum
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import MISSING_VALUE_REPLACEMENT, quoted_identifier


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
    def str_trim(
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

    @classmethod
    @abstractmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: str,
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:
        """
        Aggregate per category values into key value pairs

        # noqa: DAR103

        Input:

        --------------------------------------
        POINT_IN_TIME  ENTITY  CATEGORY  VALUE
        --------------------------------------
        2022-01-01     C1      K1        1
        2022-01-01     C1      K2        2
        2022-01-01     C2      K3        3
        2022-01-01     C3      K1        4
        ...
        --------------------------------------

        Output:

        -----------------------------------------
        POINT_IN_TIME  ENTITY  VALUE_AGG
        -----------------------------------------
        2022-01-01     C1      {"K1": 1, "K2": 2}
        2022-01-01     C2      {"K2": 3}
        2022-01-01     C3      {"K1": 4}
        ...
        -----------------------------------------

        Parameters
        ----------
        point_in_time_column : str
            Point in time column name
        serving_names : list[str]
            List of serving name columns
        value_by : str | None
            Optional category parameter for the groupby operation
        agg_result_names : list[str]
            Column names of the aggregated results
        inner_agg_result_names : list[str]
            Column names of the intermediate aggregation result names (one value per category - this
            is to be used as the values in the aggregated key-value pairs)
        inner_agg_expr : expressions.Subqueryable:
            Query that produces the intermediate aggregation result

        Returns
        -------
        str
        """

    @classmethod
    @abstractmethod
    def get_online_store_type_from_dtype(cls, dtype: DBVarType) -> str:
        """
        Get the database specific type name given a Feature's DBVarType for online store purpose

        Parameters
        ----------
        dtype : DBVarType
            Data type

        Returns
        -------
        str
        """


class SnowflakeAdapter(BaseAdapter):
    """
    Helper class to generate Snowflake specific SQL expressions
    """

    class SnowflakeOnlineStoreColumnType(StrEnum):
        """
        Possible column types in Snowflake online store tables
        """

        FLOAT = "FLOAT"
        VARCHAR = "VARCHAR"
        OBJECT = "OBJECT"
        VARIANT = "VARIANT"

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(
            this="DATE_PART",
            expressions=[expressions.Identifier(this="EPOCH_SECOND"), timestamp_expr],
        )

    @classmethod
    def str_trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        expression_class = {
            "left": fb_expressions.LTrim,
            "right": fb_expressions.RTrim,
            "both": fb_expressions.make_trim_expression,
        }[side]
        if character:
            return expression_class(this=expr, character=make_literal_value(character))  # type: ignore
        return expression_class(this=expr)  # type: ignore

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

    @classmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: str,
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:

        inner_alias = "INNER_"

        outer_group_by_keys = [f"{inner_alias}.{quoted_identifier(point_in_time_column).sql()}"]
        for serving_name in serving_names:
            outer_group_by_keys.append(f"{inner_alias}.{quoted_identifier(serving_name).sql()}")

        category_col = f"{inner_alias}.{quoted_identifier(value_by).sql()}"
        # Cast type to string first so that integer can be represented nicely ('{"0": 7}' vs
        # '{"0.00000": 7}')
        category_col_casted = f"CAST({category_col} AS VARCHAR)"
        # Replace missing category values since OBJECT_AGG ignores keys that are null
        category_filled_null = (
            f"CASE WHEN {category_col} IS NULL THEN '{MISSING_VALUE_REPLACEMENT}' ELSE "
            f"{category_col_casted} END"
        )
        object_agg_exprs = [
            f'OBJECT_AGG({category_filled_null}, TO_VARIANT({inner_alias}."{inner_agg_result_name}"))'
            f' AS "{agg_result_name}"'
            for inner_agg_result_name, agg_result_name in zip(
                inner_agg_result_names, agg_result_names
            )
        ]
        agg_expr = (
            select(*outer_group_by_keys, *object_agg_exprs)
            .from_(inner_agg_expr.subquery(alias=inner_alias))
            .group_by(*outer_group_by_keys)
        )
        return agg_expr

    @classmethod
    def get_online_store_type_from_dtype(cls, dtype: DBVarType) -> str:
        if dtype in {DBVarType.INT, DBVarType.FLOAT}:
            return cls.SnowflakeOnlineStoreColumnType.FLOAT
        if dtype == DBVarType.VARCHAR:
            return cls.SnowflakeOnlineStoreColumnType.VARCHAR
        if dtype == DBVarType.OBJECT:
            return cls.SnowflakeOnlineStoreColumnType.OBJECT
        # Currently we don't expect features to be of any other types than above. Otherwise, default
        # to VARIANT since it can hold any data types
        return cls.SnowflakeOnlineStoreColumnType.VARIANT


class DatabricksAdapter(BaseAdapter):
    """
    Helper class to generate Databricks specific SQL expressions
    """

    @classmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: str,
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:
        raise NotImplementedError()

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(this="UNIX_TIMESTAMP", expressions=[timestamp_expr])

    @classmethod
    def str_trim(
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

    @classmethod
    def get_online_store_type_from_dtype(cls, dtype: DBVarType) -> str:
        raise NotImplementedError()


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
