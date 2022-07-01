"""
Common test fixtures used across api test directories
"""
import textwrap

import pytest

from featurebyte.api.feature import Feature, FeatureGroup
from featurebyte.api.groupby import EventViewGroupBy
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


@pytest.fixture()
def expected_snowflake_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "event_timestamp" AS "event_timestamp",
          "created_at" AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
        """
    ).strip()


@pytest.fixture(name="grouped_event_view")
def grouped_event_view_fixture(snowflake_event_view):
    """
    EventViewGroupBy fixture
    """
    snowflake_event_view.cust_id.as_entity("customer")
    grouped = snowflake_event_view.groupby("cust_id")
    assert isinstance(grouped, EventViewGroupBy)
    yield grouped


@pytest.fixture(name="feature_list")
def feature_list_fixture(grouped_event_view):
    """
    FeatureList fixture
    """
    feature_list = grouped_event_view.aggregate(
        value_column="col_float",
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
            "parent": "col_float",
            "agg_func": "sum",
            "value_by": None,
            "windows": ["30m", "2h", "1d"],
            "timestamp": "event_timestamp",
            "blind_spot": 600,
            "time_modulo_frequency": 300,
            "frequency": 1800,
            "names": ["sum_30m", "sum_2h", "sum_1d"],
            "tile_id": "sum_f1800_m300_b600_2ffe099df53ee760d5a551c17707fedd0cf861f9",
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
