"""
Unit test for EventViewGroupBy
"""
import pytest

from featurebyte.core.groupby import EventViewGroupBy


@pytest.mark.parametrize(
    "keys,expected_keys",
    [
        (None, ["cust_id"]),
        ("cust_id", ["cust_id"]),
        (["cust_id", "session_id"], ["cust_id", "session_id"]),
    ],
)
def test_constructor(event_view, keys, expected_keys):
    """
    Test constructor
    """
    grouped = EventViewGroupBy(obj=event_view, keys=keys)
    assert grouped.keys == expected_keys


@pytest.mark.usefixtures("graph")
def test_constructor__missing_keys(event_view_without_entity_ids):
    """
    Test case when the constructor not able to infer the keys from the event source
    """
    assert event_view_without_entity_ids.entity_identifiers is None
    with pytest.raises(ValueError) as exc:
        EventViewGroupBy(obj=event_view_without_entity_ids, keys=None)
    expected_msg = (
        "Not able to infer keys from "
        "EventView(node.name=input_1, timestamp_column=created_at, entity_identifiers=None)!"
    )
    assert expected_msg in str(exc.value)


def test_constructor__wrong_input_type(event_view):
    """
    Test not valid object type passed to the constructor
    """
    with pytest.raises(TypeError) as exc:
        EventViewGroupBy(obj=True, keys="whatever")
    expected_msg = "Expect <class 'featurebyte.core.event_view.EventView'> object type!"
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        EventViewGroupBy(event_view, True)
    expected_msg = (
        "Grouping EventView(node.name=input_1, timestamp_column=created_at, entity_identifiers=['cust_id']) "
        "by 'True' is not supported!"
    )
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(event_view):
    """
    Test cases when column of the keys not found in the event source
    """
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=event_view, keys="random_column")
    expected_msg = (
        "Column 'random_column' not found in "
        "EventView(node.name=input_1, timestamp_column=created_at, entity_identifiers=['cust_id'])!"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=event_view, keys=["cust_id", "random_column"])
    assert expected_msg in str(exc.value)


def test_groupby__wrong_method(event_view):
    """
    Test not valid aggregation method passed to groupby
    """
    grouped = EventViewGroupBy(obj=event_view, keys=None)
    with pytest.raises(ValueError) as exc:
        grouped.aggregate("a", "unknown_method", ["1d"], "5m", "1h", "10m", ["feature_name"])
    expected_message = "Aggregation method not supported: unknown_method"
    assert expected_message in str(exc.value)
