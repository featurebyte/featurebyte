"""
Unit tests for aggregate_over
"""

import pytest

from featurebyte import FeatureJobSetting
from featurebyte.enum import DBVarType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import NodeType
from tests.util.helper import get_node


@pytest.mark.parametrize(
    "value_column, expected_dtype",
    [
        ("col_int", DBVarType.INT),
        ("col_float", DBVarType.FLOAT),
        ("col_char", DBVarType.CHAR),
        ("col_text", DBVarType.VARCHAR),
        ("col_boolean", DBVarType.BOOL),
        ("event_timestamp", DBVarType.TIMESTAMP_TZ),
        ("col_binary", DBVarType.BINARY),
    ],
)
def test_aggregate_over__latest_method_output_vartype(snowflake_event_view_with_entity, value_column, expected_dtype):
    """
    Test latest aggregation output variable type
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=value_column,
        method="latest",
        windows=["1h"],
        feature_names=["feat_1h"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    assert feature_group["feat_1h"].dtype == expected_dtype


def test_unbounded_window__valid(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test a valid case of specifying None as window size
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    feature_dict = feature_group["feat_latest"].dict()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["windows"] == [None]


def test_unbounded_window__non_latest(snowflake_event_view_with_entity):
    """
    Test window size of None is only valid for latest aggregation method
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
        )
    assert str(exc.value) == 'Unbounded window is only supported for the "latest" method'


def test_unbounded_window__category_not_supported(snowflake_event_view_with_entity):
    """
    Test aggregation per category is not supported with unbounded windows
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id", category="col_int").aggregate_over(
            value_column="col_float",
            method="latest",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
        )
    assert str(exc.value) == "category is not supported for aggregation with unbounded window"


def test_unbounded_window__composite_keys(snowflake_event_view_with_entity):
    """
    Test composite keys
    """
    feature_group = snowflake_event_view_with_entity.groupby(["cust_id", "col_int"]).aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    feature_dict = feature_group["feat_latest"].dict()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["windows"] == [None]
    assert node["parameters"]["keys"] == ["cust_id", "col_int"]


def test_empty_groupby_keys(snowflake_event_view_with_entity):
    """
    Test empty groupby keys (feature without any entity)
    """
    feature_group = snowflake_event_view_with_entity.groupby([]).aggregate_over(
        method="count",
        windows=["30d"],
        feature_names=["feat_count"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    feature_dict = feature_group["feat_count"].dict()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["keys"] == []
    assert node["parameters"]["entity_ids"] == []

    feature_model = FeatureModel(**feature_group["feat_count"].dict())
    assert feature_model.entity_ids == []


def test_offset(snowflake_event_view_with_entity):
    """
    Test offset with window aggregation
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="max",
        windows=["24h"],
        offset="12h",
        feature_names=["feature"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
    )
    feature_dict = feature_group["feature"].dict()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["offset"] == "12h"


def test_offset__invalid_duration(snowflake_event_view_with_entity):
    """
    Test offset with window aggregation
    """
    with pytest.raises(ValueError) as exc_info:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="max",
            windows=["24h"],
            offset="13m",
            feature_names=["feature"],
            feature_job_setting=FeatureJobSetting(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
        )
    assert str(exc_info.value) == "window provided 13m is not a multiple of the feature job frequency 360s"


def test_count_distinct_agg_func(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test count_distinct aggregation function produces a different node type
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="count_distinct",
        windows=["7d"],
        feature_names=["col_int_count_distinct_7d"],
        feature_job_setting=FeatureJobSetting(blind_spot="0", period="1d", offset="1h"),
    )["col_int_count_distinct_7d"]

    feature_node_name = feature.node.name
    aggregation_node_name = feature.graph.backward_edges_map[feature_node_name][0]
    aggregation_node = feature.graph.get_node_by_name(aggregation_node_name)
    assert aggregation_node.type == NodeType.NON_TILE_WINDOW_AGGREGATE
    assert aggregation_node.parameters.dict() == {
        "agg_func": "count_distinct",
        "entity_ids": [cust_id_entity.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "execution_buffer": "0s",
            "offset": "3600s",
            "period": "86400s",
        },
        "keys": ["cust_id"],
        "names": ["col_int_count_distinct_7d"],
        "offset": None,
        "parent": "col_int",
        "serving_names": ["cust_id"],
        "timestamp": "event_timestamp",
        "value_by": None,
        "windows": ["7d"],
    }

    # check feature can be saved
    feature.save()
