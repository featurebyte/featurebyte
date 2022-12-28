"""
Unit test for EventViewGroupBy
"""
from typing import List, Type

import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.groupby import BaseAggregator, GroupBy
from featurebyte.api.view import View
from featurebyte.enum import DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.node.agg_func import AggFuncType
from featurebyte.query_graph.transform.reconstruction import (
    GroupbyNode,
    add_pruning_sensitive_operation,
)


@pytest.mark.parametrize(
    "keys,expected_keys",
    [
        ("cust_id", ["cust_id"]),
        (["cust_id", "col_text"], ["cust_id", "col_text"]),
    ],
)
def test_constructor(snowflake_event_data, keys, expected_keys):
    """
    Test constructor
    """
    expected_serving_names = ["serving_" + column for column in expected_keys]

    # set key columns as entity
    for column, serving_name in zip(expected_keys, expected_serving_names):
        # create an entity for each column
        Entity(name=column, serving_names=[serving_name]).save()

        # mark the column as entity
        snowflake_event_data[column].as_entity(column)

    snowflake_event_view = EventView.from_event_data(event_data=snowflake_event_data)
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
        "featurebyte.api.item_view.ItemView, featurebyte.api.change_view.ChangeView); got bool instead"
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
    with pytest.raises(ValueError) as exc:
        grouped.aggregate_over("a", "unknown_method", ["1d"], ["feature_name"])
    expected_message = "Aggregation method not supported: unknown_method"
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
    assert "frequency, time_module_frequency and blind_spot parameters should not be None!" in str(
        exc.value
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


def test_groupby__default_feature_job_setting(snowflake_event_data, cust_id_entity):
    """
    Test default job setting from event data is used
    """
    node_name = snowflake_event_data.node.name
    id_before = snowflake_event_data.id
    snowflake_event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        )
    )

    # check internal settings
    assert snowflake_event_data.node.name == node_name
    assert snowflake_event_data.id == id_before

    snowflake_event_data.cust_id.as_entity("customer")
    event_view = EventView.from_event_data(event_data=snowflake_event_data)

    feature_group = event_view.groupby("cust_id").aggregate_over(
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
        "tile_id": "TILE_F360_M180_B90_B38E423B7A5D8829661A0D73EA8AFE71CEF3CE27",
        "aggregation_id": "sum_6e0ab0e472928aa53dc94b1a38df7f0456e89577",
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
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
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
        "tile_id": "TILE_F360_M180_B90_24BD7EC4B4940F513AE87ACD85D90244A6938BD8",
        "aggregation_id": "sum_7f69a66a52c4fd6d2005b819a8553a46439f3245",
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
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
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
            feature_job_setting=dict(
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
        ("windows", "windows is required and should be a list; got None"),
        ("feature_names", "feature_names is required and should be a list; got None"),
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
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
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
        feature_job_setting=dict(blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"),
    )
    feature = feature_group["feat_30m"]
    feature_dict = feature.dict()
    graph_edges = feature_dict["graph"]["edges"]
    # check that the two assign nodes not get pruned
    assert graph_edges == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "mul_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "mul_1", "target": "assign_1"},
        {"source": "input_1", "target": "project_2"},
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
            feature_job_setting=dict(
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
            value_column="col_float", method="sum", feature_name="my_feature"
        )
    assert str(exc.value) == "aggregate() is only available for ItemView"


class BaseAggregatorTest(BaseAggregator):
    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate"


def test_project_feature_from_groupby_node(snowflake_event_data, cust_id_entity):
    """
    Test _project_feature_from_groupby_node
    """
    snowflake_event_data.cust_id.as_entity("customer")
    event_view = EventView.from_event_data(event_data=snowflake_event_data)

    group_by = event_view.groupby("cust_id")
    aggregator = BaseAggregatorTest(group_by)
    groupby_node = add_pruning_sensitive_operation(
        graph=event_view.graph,
        node_cls=GroupbyNode,
        node_params={
            "keys": group_by.keys,
            "parent": None,
            "agg_func": AggFuncType.COUNT,
            "value_by": None,
            "windows": [],
            "timestamp": "",
            "blind_spot": 0,
            "time_modulo_frequency": 0,
            "frequency": 24,
            "names": [],
            "serving_names": [],
            "entity_ids": [],
        },
        input_node=event_view.node,
    )
    feature = aggregator._project_feature_from_groupby_node(
        AggFuncType.COUNT,
        "feature_name",
        groupby_node,
        method="sum",
        value_column=None,
        fill_value="hello",
    )
