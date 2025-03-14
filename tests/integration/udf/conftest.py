"""
Utilities for udf testing
"""

import pytest

import tests.integration.udf.bigquery.util as bigquery_util
import tests.integration.udf.snowflake.util as snowflake_util
import tests.integration.udf.spark.util as spark_util
from tests.source_types import BIGQUERY, DATABRICKS, DATABRICKS_UNITY, SNOWFLAKE, SPARK


@pytest.fixture(name="to_object", scope="session")
def to_object_fixture(source_type):
    """
    Get function to construct a Map object from a dict for the given source_type from
    """
    if source_type == SNOWFLAKE:
        return snowflake_util.to_object
    if source_type in {SPARK, DATABRICKS, DATABRICKS_UNITY}:
        return spark_util.to_object
    if source_type == BIGQUERY:
        return bigquery_util.to_object
    raise NotImplementedError()


@pytest.fixture(name="to_array", scope="session")
def to_array_fixture(source_type):
    """
    Get function to construct an array object from an array for the given source_type
    """
    if source_type == SNOWFLAKE:
        return snowflake_util.to_array
    if source_type in {SPARK, DATABRICKS, DATABRICKS_UNITY}:
        return spark_util.to_array
    if source_type == BIGQUERY:
        return bigquery_util.to_array
    raise NotImplementedError()
