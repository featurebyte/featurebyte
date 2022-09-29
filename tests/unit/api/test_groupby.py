"""
Unit test for EventViewGroupBy
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.groupby import EventViewGroupBy
from featurebyte.enum import DBVarType


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
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys=keys)
    assert grouped.keys == expected_keys
    assert grouped.serving_names == expected_serving_names


def test_constructor__non_entity_by_keys(snowflake_event_view):
    """
    Test constructor when invalid group by keys are used
    """
    with pytest.raises(ValueError) as exc:
        _ = EventViewGroupBy(obj=snowflake_event_view, keys=["cust_id"])
    assert 'Column "cust_id" is not an entity!' in str(exc.value)


def test_constructor__wrong_input_type(snowflake_event_view):
    """
    Test not valid object type passed to the constructor
    """
    with pytest.raises(TypeError) as exc:
        EventViewGroupBy(obj=True, keys="whatever")
    expected_msg = (
        'type of argument "obj" must be featurebyte.api.event_view.EventView; got bool instead'
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        EventViewGroupBy(snowflake_event_view, True)
    expected_msg = 'type of argument "keys" must be one of (str, List[str]); got bool instead'
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when column of the keys not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view_with_entity, keys="random_column")
    assert 'Column "random_column" not found!' in str(exc.value)

    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view_with_entity, keys=["cust_id", "random_column"])
    assert 'Column "random_column" not found!' in str(exc.value)


def test_groupby__value_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when value column not found in the EventView
    """
    grouped = EventViewGroupBy(obj=snowflake_event_view_with_entity, keys="cust_id")
    with pytest.raises(KeyError) as exc:
        grouped.aggregate("non_existing_column", "sum", ["1d"], ["feature_name"])
    expected_msg = 'Column "non_existing_column" not found'
    assert expected_msg in str(exc.value)


def test_groupby__category_column_not_found(snowflake_event_view_with_entity):
    """
    Test case when category column not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(
            obj=snowflake_event_view_with_entity, keys="cust_id", category="non_existing_category"
        )
    expected_msg = 'Column "non_existing_category" not found'
    assert expected_msg in str(exc.value)


def test_groupby__wrong_method(snowflake_event_view_with_entity):
    """
    Test not valid aggregation method passed to groupby
    """
    grouped = EventViewGroupBy(obj=snowflake_event_view_with_entity, keys="cust_id")
    with pytest.raises(ValueError) as exc:
        grouped.aggregate("a", "unknown_method", ["1d"], ["feature_name"])
    expected_message = "Aggregation method not supported: unknown_method"
    assert expected_message in str(exc.value)


def test_groupby__not_able_to_infer_feature_job_setting(snowflake_event_view_with_entity):
    """
    Test groupby not able to infer feature job setting
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate(
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
        snowflake_event_view_with_entity.groupby("cust_id").aggregate(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h"],
        )
    assert "Window length must be the same as the number of output feature names." in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_1h"],
        )
    expected_msg = "Window sizes or feature names contains duplicated value(s)."
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate(
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
    snowflake_event_data.update_default_feature_job_setting(
        feature_job_setting={
            "blind_spot": "1m30s",
            "frequency": "6m",
            "time_modulo_frequency": "3m",
        }
    )
    snowflake_event_data.cust_id.as_entity("customer")
    event_view = EventView.from_event_data(event_data=snowflake_event_data)

    feature_group = event_view.groupby("cust_id").aggregate(
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
    assert groupby_node.parameters == {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "windows": ["30m", "1h", "2h"],
        "names": ["feat_30m", "feat_1h", "feat_2h"],
        "blind_spot": 90,
        "frequency": 360,
        "time_modulo_frequency": 180,
        "tile_id": "sf_table_f360_m180_b90_6779d772dcc5c83e10a93ca08923844041ded978",
        "aggregation_id": "sum_90e86c3bbbf907df40feec947372b975bd94cf21",
        "timestamp": "event_timestamp",
        "value_by": None,
        "serving_names": ["cust_id"],
        "event_data_id": event_view.event_data_id,
    }


def test_groupby__category(snowflake_event_view_with_entity):
    """
    Test category parameter is captured properly
    """
    feature_group = snowflake_event_view_with_entity.groupby(
        "cust_id", category="col_int"
    ).aggregate(
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
    assert groupby_node.parameters == {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "windows": ["30m", "1h", "2h"],
        "names": ["feat_30m", "feat_1h", "feat_2h"],
        "blind_spot": 90,
        "frequency": 360,
        "time_modulo_frequency": 180,
        "tile_id": "sf_table_f360_m180_b90_995fb463dce4af3f8384b1d76cd5575e9e9c9a39",
        "aggregation_id": "sum_e26e8c735e5f25faec286a17ba3c7120df7a28ad",
        "timestamp": "event_timestamp",
        "value_by": "col_int",
        "serving_names": ["cust_id"],
        "event_data_id": snowflake_event_view_with_entity.event_data_id,
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
    ).aggregate(**aggregate_kwargs)
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
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate(
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
        == 'Specifying value column is not allowed for COUNT aggregation; try aggregate(method="count", ...)'
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
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate(**aggregate_kwargs)
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
    ).aggregate(
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
