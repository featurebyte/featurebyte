"""
Test spark adapter module
"""

from featurebyte.query_graph.sql.adapter import SparkAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest


class TestSparkAdapter(BaseAdapterTest):
    """
    Test spark adapter class
    """

    adapter = SparkAdapter()
