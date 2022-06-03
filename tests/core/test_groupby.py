"""
Unit test for EventSourceGroupBy
"""
import pytest

from featurebyte.core.groupby import EventSourceGroupBy


@pytest.mark.parametrize(
    "keys,expected_keys",
    [
        (None, ["cust_id"]),
        ("cust_id", ["cust_id"]),
        (["cust_id", "session_id"], ["cust_id", "session_id"]),
    ],
)
def test_constructor(event_source, keys, expected_keys):
    """
    Test constructor
    """
    grouped = EventSourceGroupBy(obj=event_source, keys=keys)
    assert grouped.keys == expected_keys


@pytest.mark.usefixtures("graph")
def test_constructor__missing_keys(event_source_without_entity_ids):
    """
    Test case when the constructor not able to infer the keys from the event source
    """
    assert event_source_without_entity_ids.entity_identifiers is None
    with pytest.raises(ValueError) as exc:
        EventSourceGroupBy(obj=event_source_without_entity_ids, keys=None)
    expected_msg = (
        "Not able to infer keys from "
        "EventSource(node.name=input_1, timestamp_column=created_at, entity_identifiers=None)!"
    )
    assert expected_msg in str(exc.value)


def test_constructor__wrong_input_type(event_source):
    """
    Test not valid object type passed to the constructor
    """
    with pytest.raises(TypeError) as exc:
        EventSourceGroupBy(obj=True, keys="whatever")
    expected_msg = "Expect <class 'featurebyte.core.event_source.EventSource'> object type!"
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        EventSourceGroupBy(event_source, True)
    expected_msg = (
        "Grouping EventSource(node.name=input_1, timestamp_column=created_at, entity_identifiers=['cust_id']) "
        "by 'True' is not supported!"
    )
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(event_source):
    """
    Test cases when column of the keys not found in the event source
    """
    with pytest.raises(KeyError) as exc:
        EventSourceGroupBy(obj=event_source, keys="random_column")
    expected_msg = (
        "Column 'random_column' not found in "
        "EventSource(node.name=input_1, timestamp_column=created_at, entity_identifiers=['cust_id'])!"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(KeyError) as exc:
        EventSourceGroupBy(obj=event_source, keys=["cust_id", "random_column"])
    assert expected_msg in str(exc.value)
