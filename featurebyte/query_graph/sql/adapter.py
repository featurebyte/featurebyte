from __future__ import annotations

from abc import abstractmethod

from sqlglot import Expression, expressions

from featurebyte.enum import SourceType


class BaseAdapter:
    @classmethod
    @abstractmethod
    def epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        pass


class SnowflakeAdapter(BaseAdapter):
    @classmethod
    def epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(
            this="DATE_PART", expressions=[expressions.Identifier("EPOCH_SECOND"), timestamp_expr]
        )


class DatabricksAdapter(BaseAdapter):
    @classmethod
    def epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(this="UNIX_TIMESTAMP", expressions=[timestamp_expr])


def get_sql_adapter(source_type: SourceType) -> BaseAdapter:
    if source_type == SourceType.DATABRICKS:
        return DatabricksAdapter()
    return SnowflakeAdapter()
