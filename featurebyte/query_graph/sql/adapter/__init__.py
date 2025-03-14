"""
Package for adapter classes to generate engine specific SQL expressions
"""

from __future__ import annotations

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter.base import BaseAdapter
from featurebyte.query_graph.sql.adapter.bigquery import BigQueryAdapter
from featurebyte.query_graph.sql.adapter.databricks import DatabricksAdapter
from featurebyte.query_graph.sql.adapter.databricks_unity import DatabricksUnityAdapter
from featurebyte.query_graph.sql.adapter.snowflake import SnowflakeAdapter
from featurebyte.query_graph.sql.adapter.spark import SparkAdapter

__all__ = [
    "BaseAdapter",
    "DatabricksAdapter",
    "DatabricksUnityAdapter",
    "SnowflakeAdapter",
    "SparkAdapter",
    "get_sql_adapter",
]

from featurebyte.query_graph.sql.source_info import SourceInfo


def get_sql_adapter(source_info: SourceInfo) -> BaseAdapter:
    """
    Factory that returns an engine specific adapter given source type

    Parameters
    ----------
    source_info : SourceInfo
        Source type information

    Returns
    -------
    BaseAdapter
        Instance of BaseAdapter
    """
    source_type = source_info.source_type
    if source_type == SourceType.DATABRICKS:
        return DatabricksAdapter(source_info)
    if source_type == SourceType.DATABRICKS_UNITY:
        return DatabricksUnityAdapter(source_info)
    if source_type == SourceType.SPARK:
        return SparkAdapter(source_info)
    if source_type == SourceType.BIGQUERY:
        return BigQueryAdapter(source_info)

    assert source_type == SourceType.SNOWFLAKE, f"Unsupported source type: {source_type}"
    return SnowflakeAdapter(source_info)
