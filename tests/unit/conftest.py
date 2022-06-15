"""
Common test fixtures used across unit test directories
"""
import pytest

from featurebyte.query_graph.graph import QueryGraph


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()
