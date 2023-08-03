"""
Unit test for EventViewGroupBy
"""
from typing import List, Type

import pytest

from featurebyte import FeatureJobSetting
from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.groupby import GroupBy
from featurebyte.api.view import View
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError


@pytest.mark.parametrize(
    "keys,expected_keys",
    [
        ("cust_id", ["cust_id"]),
        (["cust_id", "col_text"], ["cust_id", "col_text"]),
    ],
)
def test_constructor(snowflake_event_table, keys, expected_keys):
    """
    Test constructor
    """
    expected_serving_names = ["serving_" + column for column in expected_keys]

    # set key columns as entity
    for column, serving_name in zip(expected_keys, expected_serving_names):
        # create an entity for each column
        Entity(name=column, serving_names=[serving_name]).save()

        # mark the column as entity
        snowflake_event_table[column].as_entity(column)

    snowflake_event_view = snowflake_event_table.get_view()
    grouped = GroupBy(obj=snowflake_event_view, keys=keys)
    assert grouped.keys == expected_keys
    assert grouped.serving_names == expected_serving_names


def test_constructor__non_entity_by_keys(snowflake_event_view):
    """
    Test constructor when invalid group by keys are used
    """
    with pytest.raises(ValueError) as exc:
        _ = GroupBy(obj=snowflake_event_view, keys=["cust_id"])
    assert 'Column "cust_id" is not an entity!' in str(exc.value)


def test_constructor__wrong_input_type(snowflake_event_view):
    """
    Test not valid object type passed to the constructor
    """
    with pytest.raises(TypeError) as exc:
        GroupBy(obj=True, keys="whatever")
    expected_msg = (
        'type of argument "obj" must be one of (featurebyte.api.event_view.EventView, '
        "featurebyte.api.item_view.ItemView, featurebyte.api.change_view.ChangeView, "
        "featurebyte.api.scd_view.SCDView); got bool instead"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        GroupBy(snowflake_event_view, True)
    expected_msg = 'type of argument "keys" must be one of (str, List[str]); got bool instead'
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when column of the keys not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        GroupBy(obj=snowflake_event_view_with_entity, keys="random_column")
    assert 'Column "random_column" not found!' in str(exc.value)

    with pytest.raises(KeyError) as exc:
        GroupBy(obj=snowflake_event_view_with_entity, keys=["cust_id", "random_column"])
    assert 'Column "random_column" not found!' in str(exc.value)


def test_groupby__value_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when value column not found in the EventView
    """
    grouped = GroupBy(obj=snowflake_event_view_with_entity, keys="cust_id")
    with pytest.raises(KeyError) as exc:
        grouped.aggregate_over("non_existing_column", "sum", ["1d"], ["feature_name"])
    expected_msg = 'Column "non_existing_column" not found'
    assert expected_msg in str(exc.value)


def test_groupby__category_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when category column not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        GroupBy(
            obj=snowflake_event_view_with_entity, keys="cust_id", category="non_existing_category"
        )
    expected_msg = 'Column "non_existing_category" not found'
    assert expected_msg in str(exc.value)


def test_groupby__wrong_method(snowflake_event_view_with_entity):
    """
    Test not valid aggregation method passed to groupby
    """
    grouped = GroupBy(obj=snowflake_event_view_with_entity, keys="cust_id")
    with pytest.raises(TypeError) as exc:
        grouped.aggregate_over("a", "unknown_method", ["1d"], ["feature_name"])
    expected_message = (
        'type of argument "method" must be one of (Literal[sum, avg, min, max, count, na_count'
    )
    assert expected_message in str(exc.value)


def test_groupby__not_able_to_infer_feature_job_setting(snowflake_event_view_with_entity):
    """
    Test groupby not able to infer feature job setting
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_2h"],
        )
    assert str(exc.value) == (
        "feature_job_setting is required as the EventView does not have a default feature job setting"
    )


def test_groupby__window_sizes_issue(snowflake_event_view_with_entity):
    """
    Test groupby not able to infer feature job setting
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h"],
        )
    assert "Window length must be the same as the number of output feature names." in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_1h"],
        )
    expected_msg = "Window sizes or feature names contains duplicated value(s)."
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "1h"],
            feature_names=["feat_30m", "feat_1h", "feat_2h"],
        )
    assert expected_msg in str(exc.value)


def test_groupby__default_feature_job_setting(
    snowflake_event_view_with_entity_and_feature_job, cust_id_entity
):
    """
    Test default job setting from event table is used
    """
    feature_group = snowflake_event_view_with_entity_and_feature_job.groupby(
        "cust_id"
    ).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
    )

    # check node params
    feature = feature_group["feat_30m"]
    feature_node_name = feature.node.name
    groupby_node_name = feature.graph.backward_edges_map[feature_node_name][0]
    groupby_node = feature.graph.get_node_by_name(groupby_node_name)
    assert groupby_node.parameters.dict() == {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "windows": ["30m", "1h", "2h"],
        "names": ["feat_30m", "feat_1h", "feat_2h"],
        "blind_spot": 90,
        "frequency": 360,
        "time_modulo_frequency": 180,
        "tile_id": "TILE_F360_M180_B90_53734EDD6250B91AC4C9B2A0EB6975F2856266F9",
        "aggregation_id": "sum_43777a02c692f201769a6adfc610ddc8216ebf76",
        "timestamp": "event_timestamp",
        "value_by": None,
        "serving_names": ["cust_id"],
        "entity_ids": [cust_id_entity.id],
    }


def test_groupby__category(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test category parameter is captured properly
    """
    feature_group = snowflake_event_view_with_entity.groupby(
        "cust_id", category="col_int"
    ).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )
    # check node params
    feature = feature_group["feat_30m"]
    feature_node_name = feature.node.name
    groupby_node_name = feature.graph.backward_edges_map[feature_node_name][0]
    groupby_node = feature.graph.get_node_by_name(groupby_node_name)
    assert groupby_node.parameters.dict() == {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "windows": ["30m", "1h", "2h"],
        "names": ["feat_30m", "feat_1h", "feat_2h"],
        "blind_spot": 90,
        "frequency": 360,
        "time_modulo_frequency": 180,
        "tile_id": "TILE_F360_M180_B90_7663A3E267DDFBE0432672B58655A368D3F5684D",
        "aggregation_id": "sum_ccff31c162fcf870dbba46c9d2dfb83ded6f9b8e",
        "timestamp": "event_timestamp",
        "value_by": "col_int",
        "serving_names": ["cust_id"],
        "entity_ids": [cust_id_entity.id],
    }


@pytest.mark.parametrize("method", ["count", "na_count"])
@pytest.mark.parametrize("category", [None, "col_int"])
def test_groupby__count_features(snowflake_event_view_with_entity, method, category):
    """
    Test count features have fillna transform applied
    """
    aggregate_kwargs = dict(
        method=method,
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )
    if method != "count":
        aggregate_kwargs["value_column"] = "col_float"

    feature_group = snowflake_event_view_with_entity.groupby(
        "cust_id", category=category
    ).aggregate_over(**aggregate_kwargs)
    feature = feature_group["feat_30m"]
    feature_dict = feature.dict()
    if category is None:
        # node type changes to ALIAS because of fillna
        assert feature_dict["node_name"] == "alias_1"
        assert feature_dict["dtype"] == DBVarType.FLOAT
    else:
        assert feature_dict["node_name"] == "project_1"
        # count with category has dict like output type
        assert feature_dict["dtype"] == DBVarType.OBJECT


def test_groupby__count_feature_specify_value_column(snowflake_event_view_with_entity):
    """
    Test count aggregation cannot have value_column specified
    """
    with pytest.raises(ValueError) as exc:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="count",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_2h"],
            feature_job_setting=FeatureJobSetting(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert (
        str(exc.value)
        == "Specifying value column is not allowed for COUNT aggregation; try setting None as the value_column"
    )


@pytest.mark.parametrize(
    "missing_param, expected_error",
    [
        ("windows", "windows is required and should be a non-empty list; got None"),
        ("feature_names", "feature_names is required and should be a non-empty list; got None"),
        ("value_column", "value_column is required"),
        ("method", "method is required"),
    ],
)
def test_groupby__required_params_missing(
    snowflake_event_view_with_entity, missing_param, expected_error
):
    """
    Test errors are expected when required parameters are missing
    """
    aggregate_kwargs = dict(
        value_column="col_float",
        method="max",
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )
    aggregate_kwargs.pop(missing_param)
    with pytest.raises(ValueError) as exc:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(**aggregate_kwargs)
    assert str(exc.value) == expected_error


def test_groupby__prune(snowflake_event_view_with_entity):
    """
    Test graph pruning works properly on groupby node using derived columns
    """
    snowflake_event_view_with_entity["derived_value_column"] = (
        10 * snowflake_event_view_with_entity["col_float"]
    )
    snowflake_event_view_with_entity["derived_category"] = (
        5 * snowflake_event_view_with_entity["col_int"]
    )
    feature_group = snowflake_event_view_with_entity.groupby(
        "cust_id", category="derived_category"
    ).aggregate_over(
        value_column="derived_value_column",
        method="sum",
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )
    feature = feature_group["feat_30m"]
    feature_dict = feature.dict()
    graph_edges = feature_dict["graph"]["edges"]
    # check that the two assign nodes not get pruned
    assert graph_edges == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "project_1"},
        {"source": "project_1", "target": "mul_1"},
        {"source": "graph_1", "target": "assign_1"},
        {"source": "mul_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_2"},
        {"source": "project_2", "target": "mul_2"},
        {"source": "assign_1", "target": "assign_2"},
        {"source": "mul_2", "target": "assign_2"},
        {"source": "assign_2", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_3"},
    ]


def test_groupby__aggregation_method_does_not_support_input_var_type(
    snowflake_event_view_with_entity,
):
    """
    Test aggregation method does not support the input var type
    """
    with pytest.raises(ValueError) as exc:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_text",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_2h"],
            feature_job_setting=FeatureJobSetting(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert 'Aggregation method "sum" does not support "VARCHAR" input variable' in str(exc.value)


def test_supported_views__aggregate(snowflake_event_view_with_entity):
    """
    Test calling aggregate without time window on EventView is not allowed
    """
    with pytest.raises(AggregationNotSupportedForViewError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate(
            value_column="col_float", method=AggFunc.SUM, feature_name="my_feature"
        )
    assert str(exc.value) == "aggregate() is only available for ItemView"


class BaseAggregatorTest(BaseAggregator):
    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate"


def get_graph_edges_from_feature(feature):
    """
    Helper method to get graph edges from feature
    """
    return feature.dict()["graph"]["edges"]


@pytest.fixture(name="test_aggregator_and_sum_feature")
def get_test_aggregator_and_feature_fixture(snowflake_event_view_with_entity_and_feature_job):
    """
    Get a test aggregator
    """
    group_by = snowflake_event_view_with_entity_and_feature_job.groupby("cust_id")
    feature_group = group_by.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["feat_30m"],
    )
    feature = feature_group["feat_30m"]
    return (
        BaseAggregatorTest(
            group_by.view_obj,
            group_by.category,
            group_by.entity_ids,
            group_by.keys,
            group_by.serving_names,
        ),
        feature,
    )


def test__fill_feature_or_target_noop(test_aggregator_and_sum_feature):
    """
    Test _fill_feature_or_target
    """
    aggregator, feature = test_aggregator_and_sum_feature

    # Passing in None as fill value, and sum count type should result in a no-op
    original_edges = get_graph_edges_from_feature(feature)
    filled_feature = aggregator._fill_feature_or_target(
        feature,
        "sum",
        feature.name,
        None,
    )
    updated_edges = get_graph_edges_from_feature(filled_feature)
    assert original_edges == updated_edges  # no change in edges


@pytest.fixture(name="test_aggregator_and_count_feature")
def get_test_aggregator_and_count_feature_fixture(snowflake_event_view_with_entity_and_feature_job):
    """
    Get a test aggregator
    """
    group_by = snowflake_event_view_with_entity_and_feature_job.groupby("cust_id")
    feature_group = group_by.aggregate_over(
        method="count",
        windows=["30m"],
        feature_names=["feat_30m"],
    )
    feature = feature_group["feat_30m"]
    return (
        BaseAggregatorTest(
            group_by.view_obj,
            group_by.category,
            group_by.entity_ids,
            group_by.keys,
            group_by.serving_names,
        ),
        feature,
    )


def test__fill_feature_or_target_count_fill_with_0(test_aggregator_and_count_feature):
    """
    Test _fill_feature_or_target
    """
    aggregator, feature = test_aggregator_and_count_feature
    original_edges = get_graph_edges_from_feature(feature)
    filled_feature = aggregator._fill_feature_or_target(
        feature,
        "count",
        feature.name,
        None,
    )
    filled_edges = get_graph_edges_from_feature(filled_feature)
    assert_edges_updated_with_conditional(original_edges, filled_edges)
    node = get_node_from_feature(filled_feature, "conditional_2")
    assert node["parameters"]["value"] == 0


def get_node_from_feature(feature, node_name):
    """
    Helper function to get a node from a feature, given a node name.
    """
    feature_dict = feature.dict()
    nodes = {}
    for node in feature_dict["graph"]["nodes"]:
        nodes[node["name"]] = node
    return nodes[node_name]


def test__fill_feature_or_target_update_value_count_feature(test_aggregator_and_count_feature):
    """
    Test _fill_feature_or_target
    """
    aggregator, feature = test_aggregator_and_count_feature
    original_edges = get_graph_edges_from_feature(feature)
    value_to_update = 2
    filled_feature = aggregator._fill_feature_or_target(
        feature,
        "count",
        feature.name,
        value_to_update,
    )
    filled_edges = get_graph_edges_from_feature(filled_feature)
    assert_edges_updated_with_conditional(original_edges, filled_edges)
    node = get_node_from_feature(filled_feature, "conditional_2")
    assert node["parameters"]["value"] == value_to_update


def assert_edges_updated_with_conditional(original_edges, updated_edges):
    """
    Helper function to assert that edges have been updated
    """
    # We expect the graph to have additional conditional nodes filled up for the fillna operation
    expected_filled_edges = [
        *original_edges,
        {"source": "alias_1", "target": "is_null_2"},
        {"source": "alias_1", "target": "conditional_2"},
        {"source": "is_null_2", "target": "conditional_2"},
        {"source": "conditional_2", "target": "alias_2"},
    ]
    assert expected_filled_edges == updated_edges


def test__fill_feature_or_target_update_value_non_count_features(test_aggregator_and_count_feature):
    """
    Test _fill_feature_or_target
    """
    aggregator, feature = test_aggregator_and_count_feature
    original_edges = get_graph_edges_from_feature(feature)
    value_to_update = 2
    filled_feature = aggregator._fill_feature_or_target(
        feature,
        "sum",
        feature.name,
        value_to_update,
    )
    filled_edges = get_graph_edges_from_feature(filled_feature)
    assert_edges_updated_with_conditional(original_edges, filled_edges)
    node = get_node_from_feature(filled_feature, "conditional_2")
    assert node["parameters"]["value"] == value_to_update


def test__fill_value_not_allowed_with_category(snowflake_event_view_with_entity):
    """
    Test fill_value not allowed when category is specified
    """
    with pytest.raises(ValueError) as exc:
        _ = snowflake_event_view_with_entity.groupby("cust_id", category="col_int").aggregate_over(
            value_column="col_float",
            method="sum",
            fill_value=0,
            windows=["30m"],
            feature_names=["feat_30m"],
            feature_job_setting=FeatureJobSetting(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert str(exc.value) == "fill_value is not supported for aggregation per category"


def test__fill_value_not_allowed_with_skip_fill_na(snowflake_event_view_with_entity):
    """
    Test fill_value not allowed when skip_fill_na is True (vice versa)
    """
    with pytest.raises(ValueError) as exc:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            fill_value=0,
            skip_fill_na=True,
            windows=["30m"],
            feature_names=["feat_30m"],
            feature_job_setting=FeatureJobSetting(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    expected_error = (
        "Specifying both fill_value and skip_fill_na is not allowed;"
        " try setting fill_value to None or skip_fill_na to False"
    )
    assert str(exc.value) == expected_error
