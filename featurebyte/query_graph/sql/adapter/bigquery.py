"""
BigQueryAdapter class
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Anonymous, Expression

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter.snowflake import SnowflakeAdapter


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
    def get_uniform_distribution_expr(cls, seed: int) -> Expression:
        _ = seed
        return expressions.Anonymous(this="RAND", expressions=[])
