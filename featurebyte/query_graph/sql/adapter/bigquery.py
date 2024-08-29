"""
BigQueryAdapter class
"""

from __future__ import annotations

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Anonymous, Expression

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.sql.adapter.snowflake import SnowflakeAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_fully_qualified_function_call


class BigQueryAdapter(SnowflakeAdapter):
    """
    Helper class to generate BigQuery specific SQL expressions
    """

    source_type = SourceType.BIGQUERY

    @classmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        key_arr = Anonymous(this="ARRAY_AGG", expressions=[key_column])
        value_arr = Anonymous(this="ARRAY_AGG", expressions=[value_column])
        return Anonymous(
            this="JSON_STRIP_NULLS",
            expressions=[Anonymous(this="JSON_OBJECT", expressions=[key_arr, value_arr])],
        )

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
