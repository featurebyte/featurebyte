"""
Common test fixtures used across test files in pandas directory
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.pandas.frame import DataFrame
from featurebyte.pandas.series import Series
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(name="dataframe")
def dataframe_fixture(graph):
    """
    DataFrame test fixture
    """
    column_var_type_map = {
        "CUST_ID": DBVarType.INT,
        "PRODUCT_ACTION": DBVarType.VARCHAR,
        "VALUE": DBVarType.FLOAT,
        "MASK": DBVarType.BOOL,
    }
    node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield DataFrame(
        node=node, column_var_type_map=column_var_type_map, row_index_lineage=[node.name]
    )


@pytest.fixture()
def bool_series(dataframe):
    """
    Series with boolean var type
    """
    series = dataframe["MASK"]
    assert isinstance(series, Series)
    assert series.name == "MASK"
    assert series.var_type == DBVarType.BOOL
    yield series


@pytest.fixture()
def int_series(dataframe):
    """
    Series with integer var type
    """
    series = dataframe["CUST_ID"]
    assert isinstance(series, Series)
    assert series.name == "CUST_ID"
    assert series.var_type == DBVarType.INT
    yield series


@pytest.fixture()
def float_series(dataframe):
    """
    Series with float var type
    """
    series = dataframe["VALUE"]
    assert isinstance(series, Series)
    assert series.name == "VALUE"
    assert series.var_type == DBVarType.FLOAT
    yield series


@pytest.fixture()
def varchar_series(dataframe):
    """
    Series with string var type
    """
    series = dataframe["PRODUCT_ACTION"]
    assert isinstance(series, Series)
    assert series.name == "PRODUCT_ACTION"
    assert series.var_type == DBVarType.VARCHAR
    yield series
