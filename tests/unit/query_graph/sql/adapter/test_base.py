"""
Test base adapter module
"""

from featurebyte.query_graph.sql.adapter import BaseAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest


class TestBaseAdapter(BaseAdapterTest):
    """
    Test base adapter class
    """

    adapter = BaseAdapter()
