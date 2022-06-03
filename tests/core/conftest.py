"""
Common test fixtures used across test files in core directory
"""
from collections import namedtuple

import pytest

from featurebyte.core.event_source import EventSource
from featurebyte.core.feature import Feature, FeatureList
from featurebyte.core.frame import Frame
from featurebyte.core.groupby import EventSourceGroupBy
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


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
    Frame test fixture
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
    yield Frame(
        node=node,
        column_var_type_map=column_var_type_map,
        column_lineage_map={col: (node.name,) for col in column_var_type_map},
        row_index_lineage=(node.name,),
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


@pytest.fixture(name="session")
def fake_session():
    """
    Fake database session for testing
    """
    FakeSession = namedtuple("FakeSession", ["database_metadata", "source_type"])
    database_metadata = {
        '"trans"': {
            "cust_id": DBVarType.INT,
            "session_id": DBVarType.INT,
            "event_type": DBVarType.VARCHAR,
            "value": DBVarType.FLOAT,
            "created_at": DBVarType.INT,
        }
    }
    yield FakeSession(database_metadata=database_metadata, source_type="sqlite")


@pytest.fixture(name="event_source")
def event_source_fixture(session, graph):
    """
    EventSource fixture
    """
    event_source = EventSource.from_session(
        session=session,
        table_name='"trans"',
        timestamp_column="created_at",
        entity_identifiers=["cust_id"],
    )
    assert isinstance(event_source, EventSource)
    expected_inception_node = Node(
        name="input_1",
        type=NodeType.INPUT,
        parameters={
            "columns": ["created_at", "cust_id", "event_type", "session_id", "value"],
            "timestamp": "created_at",
            "entity_identifiers": ["cust_id"],
            "dbtable": '"trans"',
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_source.graph is graph
    assert event_source.protected_columns == {"created_at", "cust_id"}
    assert event_source.inception_node == expected_inception_node
    assert event_source.timestamp_column == "created_at"
    assert event_source.entity_identifiers == ["cust_id"]
    yield event_source


@pytest.fixture(name="event_source_without_entity_ids")
def event_source_without_entity_ids_fixture(session, graph):
    """
    EventSource fixture
    """
    event_source = EventSource.from_session(
        session=session,
        table_name='"trans"',
        timestamp_column="created_at",
    )
    assert isinstance(event_source, EventSource)
    expected_inception_node = Node(
        name="input_1",
        type=NodeType.INPUT,
        parameters={
            "columns": ["created_at", "cust_id", "event_type", "session_id", "value"],
            "timestamp": "created_at",
            "entity_identifiers": None,
            "dbtable": '"trans"',
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_source.graph is graph
    assert event_source.protected_columns == {"created_at"}
    assert event_source.inception_node == expected_inception_node
    assert event_source.timestamp_column == "created_at"
    assert event_source.entity_identifiers is None
    yield event_source


@pytest.fixture(name="grouped_event_source")
def grouped_event_source_fixture(event_source):
    """
    EventSourceGroupBy fixture
    """
    grouped = event_source.groupby("cust_id")
    assert isinstance(grouped, EventSourceGroupBy)
    yield grouped


@pytest.fixture(name="feature_list")
def feature_list_fixture(grouped_event_source):
    """
    FeatureList fixture
    """
    feature_list = grouped_event_source.aggregate(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        schedule="30 MINUTE",
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    expected_inception_node = Node(
        name="groupby_1",
        type=NodeType.GROUPBY,
        parameters={
            "keys": ["cust_id"],
            "parent": "value",
            "agg_func": "sum",
            "value_by": None,
            "windows": ["30m", "2h", "1d"],
            "timestamp": "created_at",
            "blind_spot": "10m",
            "schedule": "30 MINUTE",
            "names": ["sum_30m", "sum_2h", "sum_1d"],
        },
        output_type=NodeOutputType.FRAME,
    )
    assert isinstance(feature_list, FeatureList)
    assert feature_list.protected_columns == {"cust_id"}
    assert feature_list.inception_node == expected_inception_node
    assert feature_list.entity_identifiers == ["cust_id"]
    assert feature_list.columns == ["cust_id", "sum_1d", "sum_2h", "sum_30m"]
    assert feature_list.column_lineage_map == {
        "cust_id": ("groupby_1",),
        "sum_30m": ("groupby_1",),
        "sum_2h": ("groupby_1",),
        "sum_1d": ("groupby_1",),
    }
    yield feature_list


@pytest.fixture(name="float_feature")
def float_feature_fixture(feature_list):
    """
    Float Feature fixture
    """
    feature = feature_list["sum_1d"]
    assert isinstance(feature, Feature)
    assert feature.protected_columns == {"cust_id"}
    assert feature.inception_node == feature_list.inception_node
    yield feature


@pytest.fixture(name="bool_feature")
def bool_feature_fixture(float_feature):
    """
    Boolean Feature fixture
    """
    bool_feature = float_feature > 100.0
    assert isinstance(bool_feature, Feature)
    assert bool_feature.protected_columns == float_feature.protected_columns
    assert bool_feature.inception_node == float_feature.inception_node
    yield bool_feature
