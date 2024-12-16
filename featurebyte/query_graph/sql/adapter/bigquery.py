"""
BigQueryAdapter class
"""

from __future__ import annotations

import re
from typing import List, Optional

from sqlglot import expressions
from sqlglot.expressions import Anonymous, Expression
from typing_extensions import Literal

from featurebyte.enum import DBVarType, SourceType, StrEnum
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_fully_qualified_function_call, sql_to_string
from featurebyte.typing import DatetimeSupportedPropertyType


class BigQueryAdapter(BaseAdapter):
    """
    Helper class to generate BigQuery specific SQL expressions
    """

    source_type = SourceType.BIGQUERY

    TABLESAMPLE_SUPPORTS_VIEW = False

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
    TIMEZONE_DATE_FORMAT_EXPRESSIONS = ["%z", "%Z", "%Ez"]

    class DataType(StrEnum):
        """
        Possible column types in BigQuery online store tables
        """

        FLOAT = "FLOAT64"
        OBJECT = "JSON"
        TIMESTAMP = "TIMESTAMP"
        STRING = "STRING"
        ARRAY = "ARRAY<FLOAT64>"

    @classmethod
    def get_physical_type_from_dtype(cls, dtype: DBVarType) -> str:
        mapping = {
            DBVarType.INT: cls.DataType.FLOAT,
            DBVarType.FLOAT: cls.DataType.FLOAT,
            DBVarType.VARCHAR: cls.DataType.STRING,
            DBVarType.TIMESTAMP: cls.DataType.TIMESTAMP,
            DBVarType.TIMESTAMP_TZ: cls.DataType.TIMESTAMP,
            DBVarType.ARRAY: cls.DataType.ARRAY,
            DBVarType.EMBEDDING: cls.DataType.ARRAY,
        }
        for dict_dtype in DBVarType.dictionary_types():
            mapping[dict_dtype] = cls.DataType.OBJECT
        if dtype in mapping:
            return mapping[dtype]
        return cls.DataType.STRING

    @classmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        key_arr = Anonymous(this="ARRAY_AGG", expressions=[key_column])
        value_arr = Anonymous(this="ARRAY_AGG", expressions=[value_column])
        aggregated_json = Anonymous(
            this="JSON_STRIP_NULLS",
            expressions=[Anonymous(this="JSON_OBJECT", expressions=[key_arr, value_arr])],
        )
        # JSON_OBJECT errors out if the array is null, so we need to handle that case
        empty_json_if_null_array = expressions.If(
            this=expressions.Is(this=value_arr, expression=expressions.Null()),
            true=Anonymous(this="JSON_OBJECT", expressions=[]),
        )
        return expressions.Case(ifs=[empty_json_if_null_array], default=aggregated_json)

    @classmethod
    def get_percentile_expr(cls, input_expr: Expression, quantile: float) -> Expression:
        return expressions.Bracket(
            this=Anonymous(
                this="APPROX_QUANTILES",
                expressions=[input_expr, make_literal_value(100)],
            ),
            expressions=[
                Anonymous(
                    this="offset",
                    expressions=[make_literal_value(int(quantile * 100))],
                )
            ],
        )

    @classmethod
    def get_uniform_distribution_expr(cls, seed: int) -> Expression:
        _ = seed
        return expressions.Anonymous(this="RAND", expressions=[])

    @classmethod
    def count_if(cls, condition: Expression) -> Expression:
        return expressions.Anonymous(this="COUNTIF", expressions=[condition])

    @classmethod
    def cast_to_string(cls, expr: Expression, dtype: Optional[DBVarType]) -> Expression:
        if dtype is not None and dtype in DBVarType.json_conversion_types():
            expr = Anonymous(this="TO_JSON_STRING", expressions=[expr])
        return super().cast_to_string(expr, dtype)

    def call_udf(self, udf_name: str, args: list[Expression]) -> Expression:
        return get_fully_qualified_function_call(
            database_name=self.source_info.database_name,
            schema_name=self.source_info.schema_name,
            function_name=udf_name,
            args=args,
        )

    @classmethod
    def prepare_before_count_distinct(cls, expr: Expression, dtype: DBVarType) -> Expression:
        if dtype in DBVarType.json_conversion_types():
            return Anonymous(this="TO_JSON_STRING", expressions=[expr])
        return expr

    @classmethod
    def lag_ignore_nulls(
        cls, expr: Expression, partition_by: List[Expression], order: Expression
    ) -> Expression:
        return expressions.Window(
            this=expressions.Anonymous(
                this="LAST_VALUE", expressions=[expressions.IgnoreNulls(this=expr)]
            ),
            partition_by=partition_by,
            order=order,
            spec=expressions.WindowSpec(
                kind="ROWS",
                start="UNBOUNDED",
                start_side="PRECEDING",
                end=make_literal_value(1),
                end_side="PRECEDING",
            ),
        )

    @classmethod
    def convert_to_utc_timestamp(cls, timestamp_expr: Expression) -> Expression:
        return cls._ensure_datetime(cls._ensure_timestamp_tz(timestamp_expr))

    @classmethod
    def from_epoch_seconds(cls, timestamp_epoch_expr: Expression) -> Expression:
        return cls._ensure_datetime(
            Anonymous(
                this="TIMESTAMP_SECONDS",
                expressions=[
                    expressions.Cast(
                        this=timestamp_epoch_expr, to=expressions.DataType.build("BIGINT")
                    )
                ],
            )
        )

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return Anonymous(
            this="UNIX_SECONDS",
            expressions=[
                expressions.Cast(this=timestamp_expr, to=expressions.DataType.build("TIMESTAMPTZ"))
            ],
        )

    @classmethod
    def _ensure_datetime(cls, expr: Expression) -> Expression:
        # Casts the expression to a BigQuery's DATETIME type (timestamp without timezone). It is
        # represented in sqlglot's TIMESTAMP type.
        return expressions.Cast(this=expr, to=expressions.DataType.build("TIMESTAMP"))

    @classmethod
    def _ensure_timestamp_tz(cls, expr: Expression) -> Expression:
        # Casts the expression to a BigQuery's TIMESTAMP type (timestamp with UTC timezone). It is
        # represented in sqlglot's TIMESTAMPTZ type.
        return expressions.Cast(this=expr, to=expressions.DataType.build("TIMESTAMPTZ"))

    @classmethod
    def _dateadd_helper(
        cls, quantity_expr: Expression, timestamp_expr: Expression, unit: str
    ) -> Expression:
        return expressions.DatetimeAdd(
            this=cls._ensure_datetime(timestamp_expr),
            expression=expressions.Cast(
                this=quantity_expr, to=expressions.DataType.build("BIGINT")
            ),
            unit=expressions.Var(this=unit),
        )

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        return cls._dateadd_helper(quantity_expr, timestamp_expr, "MICROSECOND")

    @classmethod
    def dateadd_second(cls, quantity_expr: Expression, timestamp_expr: Expression) -> Expression:
        return cls._dateadd_helper(quantity_expr, timestamp_expr, "SECOND")

    @classmethod
    def datediff_microsecond(
        cls, timestamp_expr_1: Expression, timestamp_expr_2: Expression
    ) -> Expression:
        # Need to calculate timestamp_expr_2 - timestamp_expr_1
        return expressions.TimestampDiff(
            this=cls._ensure_datetime(timestamp_expr_2),
            expression=cls._ensure_datetime(timestamp_expr_1),
            unit=expressions.Var(this="MICROSECOND"),
        )

    @classmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        # pandas: Monday=0, Sunday=6; bigquery: Sunday=1, Saturday=7
        # Conversion formula: (bigquery_dayofweek - 1 + 6) % 7
        return cls.modulo(
            expressions.Paren(
                this=expressions.Add(this=extracted_expr, expression=make_literal_value(5))
            ),
            make_literal_value(7),
        )

    @classmethod
    def get_datetime_extract_property(cls, property_type: DatetimeSupportedPropertyType) -> str:
        if property_type == "week":
            return "isoweek"
        return super().get_datetime_extract_property(property_type)

    @classmethod
    def alter_table_add_columns(
        cls,
        table: expressions.Table,
        columns: List[expressions.ColumnDef],
    ) -> str:
        alter_table_sql = f"ALTER TABLE {sql_to_string(table, source_type=cls.source_type)}\n"
        add_column_lines = []
        for column in columns:
            add_column_lines.append(
                "  ADD COLUMN " + sql_to_string(column, source_type=cls.source_type)
            )
        alter_table_sql += ",\n".join(add_column_lines)
        return alter_table_sql

    @classmethod
    def str_contains(cls, expr: Expression, pattern: str) -> Expression:
        return Anonymous(this="REGEXP_CONTAINS", expressions=[expr, make_literal_value(pattern)])

    @classmethod
    def modulo(cls, expr1: Expression, expr2: Expression) -> Expression:
        # The % operator doesn't work in BigQuery
        return expressions.Anonymous(this="MOD", expressions=[expr1, expr2])

    @classmethod
    def normalize_timestamp_before_comparison(cls, expr: Expression) -> Expression:
        return cls._ensure_datetime(expr)

    @classmethod
    def in_array(cls, input_expression: Expression, array_expression: Expression) -> Expression:
        return expressions.Exists(
            this=expressions.select(make_literal_value(1))
            .from_(
                expressions.Unnest(
                    expressions=[array_expression],
                    alias=expressions.TableAlias(columns=["element"]),
                )
            )
            .where(expressions.EQ(this="element", expression=input_expression))
        )

    @classmethod
    def object_keys(cls, dictionary_expression: Expression) -> Expression:
        return Anonymous(
            this="JSON_KEYS",
            expressions=[dictionary_expression],
        )

    def get_value_from_dictionary(
        self, dictionary_expression: Expression, key_expression: Expression
    ) -> Expression:
        # Cannot use the built-in JSON_VALUE because a dynamic expression like key_expression cannot
        # be used as the JSONPath argument (only string literal is allowed).
        return Anonymous(
            this="LAX_FLOAT64",
            expressions=[
                expressions.ParseJSON(
                    this=self.call_udf(  # result here is the value in JSON formatted string
                        "F_GET_VALUE", [dictionary_expression, key_expression]
                    ),
                    expression=expressions.Kwarg(
                        this=expressions.Var(this="wide_number_mode"),
                        expression=make_literal_value("round"),
                    ),
                )
            ],
        )

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    @classmethod
    def current_timestamp(cls) -> Expression:
        return cls._ensure_datetime(expressions.Anonymous(this="CURRENT_TIMESTAMP"))

    @classmethod
    def escape_quote_char(cls, query: str) -> str:
        # Databricks sql escapes ' with \'. Use regex to make it safe to call this more than once.
        return re.sub(r"(?<!\\)'", "\\'", query)

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
    def radian_expr(cls, expr: Expression) -> Expression:
        return expressions.Div(
            this=expressions.Mul(
                this=Anonymous(this="ACOS", expressions=[make_literal_value(-1)]),
                expression=expr,
            ),
            expression=make_literal_value(180),
        )

    @classmethod
    def to_timestamp_from_string(cls, expr: Expression, format_string: str) -> Expression:
        return expressions.Anonymous(
            this="PARSE_DATETIME",
            expressions=[make_literal_value(format_string), expr],
        )

    @classmethod
    def convert_timezone_to_utc(
        cls, expr: Expression, timezone: Expression, timezone_type: Literal["name", "offset"]
    ) -> Expression:
        _ = timezone_type
        return cls._ensure_datetime(
            expressions.Anonymous(
                this="TIMESTAMP",
                expressions=[cls._ensure_datetime(expr), timezone],
            )
        )
