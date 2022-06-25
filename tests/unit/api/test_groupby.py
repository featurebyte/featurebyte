"""
Unit test for EventViewGroupBy
"""
import pytest

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
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys=keys)
    assert grouped.keys == expected_keys


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
    expected_msg = (
        "Grouping EventView(node.name=input_2, timestamp_column=event_timestamp, entity_identifiers=[]) "
        "by 'True' is not supported!"
    )
    assert expected_msg in str(exc.value)


def test_constructor__keys_column_not_found(snowflake_event_view):
    """
    Test cases when column of the keys not found in the EventView
    """
    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view, keys="random_column")
    expected_msg = (
        "Column 'random_column' not found in "
        "EventView(node.name=input_2, timestamp_column=event_timestamp, entity_identifiers=[])!"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(KeyError) as exc:
        EventViewGroupBy(obj=snowflake_event_view, keys=["cust_id", "random_column"])
    assert expected_msg in str(exc.value)


def test_groupby__value_column_not_found(snowflake_event_view):
    """
    Test cases when value column not found in the EventView
    """
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys="cust_id")
    with pytest.raises(KeyError) as exc:
        grouped.aggregate(
            "non_existing_column", "count", ["1d"], "5m", "1h", "10m", ["feature_name"]
        )
    expected_msg = "Column 'non_existing_column' not found"
    assert expected_msg in str(exc.value)


def test_groupby__wrong_method(snowflake_event_view):
    """
    Test not valid aggregation method passed to groupby
    """
    grouped = EventViewGroupBy(obj=snowflake_event_view, keys="cust_id")
    with pytest.raises(ValueError) as exc:
        grouped.aggregate("a", "unknown_method", ["1d"], "5m", "1h", "10m", ["feature_name"])
    expected_message = "Aggregation method not supported: unknown_method"
    assert expected_message in str(exc.value)
