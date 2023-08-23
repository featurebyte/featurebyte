"""
Test snowflake adapter module
"""

from featurebyte.query_graph.sql.adapter import SnowflakeAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest


class TestSnowflakeAdapter(BaseAdapterTest):
    """
    Test snowflake adapter class
    """

    adapter = SnowflakeAdapter()
