"""
DatabricksAdapter class for generating Databricks specific SQL expressions
"""

from __future__ import annotations

from typing import Optional
from typing_extensions import Literal

import re

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import DBVarType, SourceType, StrEnum
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter.base import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, quoted_identifier


class DatabricksAdapter(BaseAdapter):
    """
    Helper class to generate Databricks specific SQL expressions
    """

    source_type = SourceType.DATABRICKS

    class DataType(StrEnum):
        """
        Possible column types in DataBricks
        """

        FLOAT = "DOUBLE"
        TIMESTAMP = "TIMESTAMP"
        STRING = "STRING"
        MAP = "MAP<STRING, DOUBLE>"
        ARRAY = "ARRAY<DOUBLE>"

    @classmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        struct_expr = expressions.Anonymous(this="struct", expressions=[key_column, value_column])
        map_expr = expressions.Anonymous(
            this="map_from_entries",
            expressions=[expressions.Anonymous(this="collect_list", expressions=[struct_expr])],
        )
        # exclude entries with null values
        return expressions.Anonymous(
            this="map_filter",
            expressions=[
                map_expr,
                expressions.Lambda(
                    this=expressions.Not(
                        this=expressions.Is(
                            this=expressions.Identifier(this="v"), expression=expressions.Null()
                        )
                    ),
                    expressions=[
                        expressions.Identifier(this="k"),
                        expressions.Identifier(this="v"),
                    ],
                ),
            ],
        )

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
    def to_seconds_double(cls, expr: Expression) -> Expression:
        """
        Convert date or timestamp expression to floating point epoch seconds while preserving
        sub-seconds component

        Parameters
        ----------
        expr: Expression
            Date or timestamp expression

        Returns
        -------
        Expression
        """
        # Date type must be converted to TIMESTAMP type first before casting to double, else it
        # returns NA.
        timestamp_expr = expressions.Cast(this=expr, to=expressions.DataType.build("TIMESTAMP"))
        timestamp_seconds = expressions.Cast(
            this=timestamp_expr, to=expressions.DataType.build("DOUBLE")
        )
        return timestamp_seconds

    @classmethod
    def _dateadd_by_casting_to_seconds(
        cls,
        quantity_expr: Expression,
        timestamp_expr: Expression,
        quantity_scale: Optional[float] = None,
    ) -> Expression:
        # DATEADD with a customisable unit is not supported in older versions of Spark (< 3.3). To
        # workaround that, convert to double (epoch seconds with sub-seconds preserved) to perform
        # the addition and then convert back to timestamp.
        timestamp_seconds = cls.to_seconds_double(timestamp_expr)
        if quantity_scale is None:
            seconds_quantity = quantity_expr
        else:
            seconds_quantity = expressions.Div(
                this=quantity_expr, expression=make_literal_value(quantity_scale)
            )
        timestamp_seconds_added = expressions.Add(
            this=timestamp_seconds, expression=seconds_quantity
        )
        # Note: FROM_UNIXTIME doesn't work as it discards sub-seconds components even if sub-seconds
        # are included in the specified date format.
        return expressions.Cast(
            this=timestamp_seconds_added, to=expressions.DataType.build("TIMESTAMP")
        )

    @classmethod
    def dateadd_second(cls, quantity_expr: Expression, timestamp_expr: Expression) -> Expression:
        return cls._dateadd_by_casting_to_seconds(quantity_expr, timestamp_expr)

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        return cls._dateadd_by_casting_to_seconds(quantity_expr, timestamp_expr, quantity_scale=1e6)

    @classmethod
    def get_physical_type_from_dtype(cls, dtype: DBVarType) -> str:
        mapping = {
            DBVarType.INT: cls.DataType.FLOAT,
            DBVarType.FLOAT: cls.DataType.FLOAT,
            DBVarType.VARCHAR: cls.DataType.STRING,
            DBVarType.OBJECT: cls.DataType.MAP,
            DBVarType.TIMESTAMP: cls.DataType.TIMESTAMP,
            DBVarType.ARRAY: cls.DataType.ARRAY,
            DBVarType.EMBEDDING: cls.DataType.ARRAY,
        }
        if dtype in mapping:
            return mapping[dtype]
        return cls.DataType.STRING

    @classmethod
    def object_keys(cls, dictionary_expression: Expression) -> Expression:
        return expressions.Anonymous(this="map_keys", expressions=[dictionary_expression])

    @classmethod
    def in_array(cls, input_expression: Expression, array_expression: Expression) -> Expression:
        return expressions.Anonymous(
            this="array_contains", expressions=[array_expression, input_expression]
        )

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    @classmethod
    def get_value_from_dictionary(
        cls, dictionary_expression: Expression, key_expression: Expression
    ) -> Expression:
        return expressions.Bracket(this=dictionary_expression, expressions=[key_expression])

    @classmethod
    def convert_to_utc_timestamp(cls, timestamp_expr: Expression) -> Expression:
        # timestamps do not have timezone information
        return expressions.Cast(this=timestamp_expr, to=expressions.DataType.build("TIMESTAMP"))

    @classmethod
    def current_timestamp(cls) -> Expression:
        return expressions.Anonymous(this="current_timestamp")

    @classmethod
    def escape_quote_char(cls, query: str) -> str:
        # Databricks sql escapes ' with \'. Use regex to make it safe to call this more than once.
        return re.sub(r"(?<!\\)'", "\\'", query)

    @classmethod
    def create_table_as(
        cls,
        table_details: TableDetails,
        select_expr: Select | str,
        kind: Literal["TABLE", "VIEW"] = "TABLE",
        partition_keys: list[str] | None = None,
        replace: bool = False,
    ) -> Expression:
        """
        Construct query to create a table using a select statement

        Parameters
        ----------
        table_details: TableDetails
            TableDetails of the table to be created
        select_expr: Select | str
            Select expression
        kind: Literal["TABLE", "VIEW"]
            Kind of table to create
        partition_keys: list[str] | None
            Partition keys
        replace: bool
            Whether to replace the table if exists

        Returns
        -------
        Expression
        """
        destination_expr = get_fully_qualified_table_name(table_details.dict())

        if kind == "TABLE":
            table_properties = [
                expressions.TableFormatProperty(this=expressions.Var(this="DELTA")),
                expressions.Property(
                    this=expressions.Literal(this="delta.columnMapping.mode"), value="'name'"
                ),
                expressions.Property(
                    this=expressions.Literal(this="delta.minReaderVersion"), value="'2'"
                ),
                expressions.Property(
                    this=expressions.Literal(this="delta.minWriterVersion"), value="'5'"
                ),
            ]
            if partition_keys:
                table_properties.append(
                    expressions.PartitionedByProperty(
                        this=expressions.Schema(
                            expressions=[quoted_identifier(key) for key in partition_keys]
                        )
                    )
                )
            properties = expressions.Properties(expressions=table_properties)
        else:
            properties = None

        return expressions.Create(
            this=expressions.Table(this=destination_expr),
            kind=kind,
            expression=select_expr,
            properties=properties,
            replace=replace,
        )

    @classmethod
    def any_value(cls, expr: Expression) -> Expression:
        return expressions.Anonymous(this="first", expressions=[expr])

    @classmethod
    def get_percentile_expr(cls, input_expr: Expression, quantile: float) -> Expression:
        return expressions.Anonymous(
            this="percentile", expressions=[input_expr, make_literal_value(quantile)]
        )

    @classmethod
    def get_uniform_distribution_expr(cls, seed: int) -> Expression:
        return expressions.Anonymous(this="RANDOM", expressions=[make_literal_value(seed)])
