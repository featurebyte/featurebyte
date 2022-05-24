"""
Common fixtures used for testing pandas like operations
"""
import pytest

from featurebyte.execution_graph.graph import ExecutionGraph
from featurebyte.pandas.frame import DataFrame


@pytest.fixture()
def source_df():
    ExecutionGraph.clear()
    yield DataFrame.from_source()
