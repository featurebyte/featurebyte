"""
Common test fixtures used across api test directories
"""
import pytest

from featurebyte.api.database_source import DatabaseSource
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature, FeatureGroup
from featurebyte.api.groupby import EventViewGroupBy
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


@pytest.fixture(name="database_source")
def database_source_fixture(snowflake_datasource):
    """
    Database source fixture
    """
    return DatabaseSource(**snowflake_datasource.dict())


@pytest.fixture(name="database_table")
def database_table_fixture(snowflake_connector, snowflake_execute_query, database_source, config):
    """
    Test retrieval database table by indexing
    """
    _ = snowflake_connector, snowflake_execute_query
    yield database_source["sf_table", config]


@pytest.fixture(name="event_view")
def event_view_fixture(session, graph):
    """
    EventView fixture
    """
    event_view = EventView.from_session(
        session=session,
        table_name='"trans"',
        timestamp_column="created_at",
        entity_identifiers=["cust_id"],
    )
    assert isinstance(event_view, EventView)
    expected_inception_node = Node(
        name="input_1",
        type=NodeType.INPUT,
        parameters={
            "columns": ["cust_id", "session_id", "event_type", "value", "created_at"],
            "timestamp": "created_at",
            "entity_identifiers": ["cust_id"],
            "dbtable": '"trans"',
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_view.graph.dict() == graph.dict()
    assert event_view.protected_columns == {"created_at", "cust_id"}
    assert event_view.inception_node == expected_inception_node
    assert event_view.timestamp_column == "created_at"
    assert event_view.entity_identifiers == ["cust_id"]
    yield event_view


@pytest.fixture(name="event_view_without_entity_ids")
def event_view_without_entity_ids_fixture(session, graph):
    """
    EventView fixture
    """
    event_view = EventView.from_session(
        session=session,
        table_name='"trans"',
        timestamp_column="created_at",
    )
    assert isinstance(event_view, EventView)
    expected_inception_node = Node(
        name="input_1",
        type=NodeType.INPUT,
        parameters={
            "columns": ["cust_id", "session_id", "event_type", "value", "created_at"],
            "timestamp": "created_at",
            "entity_identifiers": None,
            "dbtable": '"trans"',
        },
        output_type=NodeOutputType.FRAME,
    )
    assert event_view.graph.dict() == graph.dict()
    assert event_view.protected_columns == {"created_at"}
    assert event_view.inception_node == expected_inception_node
    assert event_view.timestamp_column == "created_at"
    assert event_view.entity_identifiers is None
    yield event_view


@pytest.fixture(name="grouped_event_view")
def grouped_event_view_fixture(event_view):
    """
    EventViewGroupBy fixture
    """
    grouped = event_view.groupby("cust_id")
    assert isinstance(grouped, EventViewGroupBy)
    yield grouped


@pytest.fixture(name="feature_list")
def feature_list_fixture(grouped_event_view):
    """
    FeatureList fixture
    """
    feature_list = grouped_event_view.aggregate(
        value_column="value",
        method="sum",
        windows=["30m", "2h", "1d"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
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
            "blind_spot": 600,
            "time_modulo_frequency": 300,
            "frequency": 1800,
            "names": ["sum_30m", "sum_2h", "sum_1d"],
        },
        output_type=NodeOutputType.FRAME,
    )
    assert isinstance(feature_list, FeatureGroup)
    assert feature_list.protected_columns == {"cust_id"}
    assert feature_list.inception_node == expected_inception_node
    assert feature_list.entity_identifiers == ["cust_id"]
    assert feature_list.columns == ["cust_id", "sum_30m", "sum_2h", "sum_1d"]
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
