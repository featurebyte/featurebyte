"""
Package for adapter classes to generate engine specific SQL expressions
"""

from __future__ import annotations

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter.base import BaseAdapter
from featurebyte.query_graph.sql.adapter.databricks import DatabricksAdapter
from featurebyte.query_graph.sql.adapter.snowflake import SnowflakeAdapter
from featurebyte.query_graph.sql.adapter.spark import SparkAdapter

__all__ = [
    "BaseAdapter",
    "DatabricksAdapter",
    "SnowflakeAdapter",
    "SparkAdapter",
    "get_sql_adapter",
]


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
    if source_type in {SourceType.DATABRICKS, SourceType.DATABRICKS_UNITY}:
        return DatabricksAdapter()
    if source_type == SourceType.SPARK:
        return SparkAdapter()
    return SnowflakeAdapter()
