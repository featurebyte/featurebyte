"""
Custom sqlglot dialects
"""

from __future__ import annotations

from sqlglot.dialects.databricks import Databricks
from sqlglot.dialects.dialect import DialectType
from sqlglot.dialects.spark import Spark

from featurebyte.enum import SourceType


class CustomSpark(Spark):
    """
    Custom Spark dialect
    """

    class Generator(Spark.Generator):
        """
        Custom Spark generator
        """

        EXPRESSIONS_WITHOUT_NESTED_CTES = set()


class CustomDatabricks(Databricks):
    """
    Custom Databricks dialect
    """

    class Generator(Databricks.Generator):
        """
        Custom Databricks generator
        """

        EXPRESSIONS_WITHOUT_NESTED_CTES = set()


def get_dialect_from_source_type(source_type: SourceType) -> DialectType:
    """
    Get the dialect class given SourceType

    Parameters
    ----------
    source_type : SourceType
        Source type information

    Returns
    -------
    Type[DialectType]
    """
    mapping: dict[SourceType, DialectType] = {
        SourceType.SPARK: CustomSpark,
        SourceType.DATABRICKS: CustomDatabricks,
        SourceType.DATABRICKS_UNITY: CustomDatabricks,
        SourceType.BIGQUERY: "bigquery",
        SourceType.SNOWFLAKE: "snowflake",
    }
    return mapping[source_type]
