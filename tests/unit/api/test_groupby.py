"""
Unit test for EventViewGroupBy
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.groupby import EventViewGroupBy


@pytest.mark.parametrize(
    "keys,expected_keys",
    [
        ("cust_id", ["cust_id"]),
        (["cust_id", "col_text"], ["cust_id", "col_text"]),
    ],
)
def test_constructor(snowflake_event_view, keys, expected_keys):
    """
    Test constructor
    """
    # set key columns as entity
    for column in expected_keys:
        # create an entity for each column
        Entity.create(name=column, serving_name=column)

        # mark the column as entity
        snowflake_event_view[column].as_entity(column)

    grouped = EventViewGroupBy(obj=snowflake_event_view, keys=keys)
    assert grouped.keys == expected_keys


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
    expected_msg = "Expect <class 'featurebyte.api.event_view.EventView'> object type!"
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        EventViewGroupBy(snowflake_event_view, True)
    expected_msg = 'Grouping EventView(node.name=input_2, timestamp_column=event_timestamp) by "True" is not supported!'
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(snowflake_event_view):
    """
    Test cases when column of the keys not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view, keys="random_column")
    assert 'Column "random_column" not found!' in str(exc.value)

    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view, keys=["cust_id", "random_column"])
    assert 'Column "random_column" not found!' in str(exc.value)


def test_groupby__value_column_not_found(snowflake_event_view):
    """
    Test cases when value column not found in the EventView
    """
    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys="cust_id")
    with pytest.raises(KeyError) as exc:
        grouped.aggregate(
            "non_existing_column", "count", ["1d"], "5m", "1h", "10m", ["feature_name"]
        )
    expected_msg = 'Column "non_existing_column" not found'
    assert expected_msg in str(exc.value)


def test_groupby__wrong_method(snowflake_event_view):
    """
    Test not valid aggregation method passed to groupby
    """
    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys="cust_id")
    with pytest.raises(ValueError) as exc:
        grouped.aggregate("a", "unknown_method", ["1d"], "5m", "1h", "10m", ["feature_name"])
    expected_message = "Aggregation method not supported: unknown_method"
    assert expected_message in str(exc.value)


def test_groupby__not_able_to_infer_feature_job_setting(snowflake_event_view):
    """
    Test groupby not able to infer feature job setting
    """
    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    with pytest.raises(ValueError) as exc:
        snowflake_event_view.groupby("cust_id").aggregate(
            value_column="col_float",
            method="sum",
            windows=["30m", "1h", "2h"],
            feature_names=["feat_30m", "feat_1h", "feat_2h"],
        )
    assert "frequency, time_module_frequency and blind_spot parameters should not be None!" in str(
        exc.value
    )


def test_groupby__default_feature_job_setting(snowflake_event_data):
    """
    Test default job setting from event data is used
    """
    snowflake_event_data.update_default_feature_job_setting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    event_view = EventView.from_event_data(event_data=snowflake_event_data)

    Entity.create(name="customer", serving_name="cust_id")
    event_view.cust_id.as_entity("customer")
    feature_group = event_view.groupby("cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m", "1h", "2h"],
        feature_names=["feat_30m", "feat_1h", "feat_2h"],
    )

    # check node params
    assert feature_group.node.parameters == {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "windows": ["30m", "1h", "2h"],
        "names": ["feat_30m", "feat_1h", "feat_2h"],
        "blind_spot": 90,
        "frequency": 360,
        "time_modulo_frequency": 180,
        "tile_id": "sum_f360_m180_b90_45058a98f2f07813bc673622af9c945b2ceaa16d",
        "timestamp": "event_timestamp",
        "value_by": None,
    }
