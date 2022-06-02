"""
Unit test for EventSource
"""
import pytest

from featurebyte.core.event_source import EventSource
from featurebyte.core.groupby import EventSourceGroupBy
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType


def test_event_source__table_key_not_found(session):
    """
    Test EventSource creation when table key not found
    """
    with pytest.raises(KeyError) as exc:
        EventSource.from_session(
            session=session,
            table_name='"random_database"."random_table"',
            timestamp_column="some_random_column",
            entity_identifiers=["some_random_id"],
        )
    expected_msg = 'Could not find the "random_database"."random_table" table!'
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize(
    "timestamp_column,entity_identifiers,expected_column",
    [
        ("some_random_column", ["whatever"], "some_random_column"),
        ("created_at", ["cust_id", "some_random_id"], "some_random_id"),
    ],
)
def test_event_source__column_not_found(
    session, timestamp_column, entity_identifiers, expected_column
):
    """
    Test EventSource creation when timestamp column not found
    """
    with pytest.raises(KeyError) as exc:
        EventSource.from_session(
            session=session,
            table_name='"trans"',
            timestamp_column=timestamp_column,
            entity_identifiers=entity_identifiers,
        )
    expected_msg = f'Could not find the "{expected_column}" column from the table "trans"!'
    assert expected_msg in str(exc.value)


def test_getitem__list_of_str(event_source):
    """
    Test retrieving subset of the event source features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_source_subset1 = event_source[["value"]]
    assert isinstance(event_source_subset1, EventSource)
    assert set(event_source_subset1.column_var_type_map) == {"created_at", "cust_id", "value"}
    assert event_source_subset1.row_index_lineage == event_source.row_index_lineage
    assert event_source_subset1.inception_node == event_source.inception_node

    # case 2: select a non-protected column with a timestamp column
    event_source_subset2 = event_source[["value", "created_at"]]
    assert isinstance(event_source_subset2, EventSource)
    assert set(event_source_subset2.column_var_type_map) == {"created_at", "cust_id", "value"}
    assert event_source_subset2.row_index_lineage == event_source.row_index_lineage
    assert event_source_subset2.inception_node == event_source.inception_node

    # both event source subsets actually point to the same node
    assert event_source_subset1.node == event_source_subset2.node


def test_getitem__series_key(event_source):
    """
    Test filtering on event source object
    """
    mask_cust_id = event_source["cust_id"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.var_type == DBVarType.BOOL

    event_source_row_subset = event_source[mask_cust_id]
    assert isinstance(event_source_row_subset, EventSource)
    assert event_source_row_subset.row_index_lineage == ("input_1", "filter_1")
    assert event_source_row_subset.inception_node == event_source.inception_node


@pytest.mark.parametrize("column", ["created_at", "cust_id"])
def test_setitem__override_protected_column(event_source, column):
    """
    Test attempting to change event source's timestamp value or entity identifier value
    """
    with pytest.raises(ValueError) as exc:
        event_source[column] = 1
    expected_msg = "Not allow to override timestamp column or entity identifiers!"
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize("by", ["cust_id", ["cust_id", "session_id"]])
def test_groupby(event_source, by):
    """
    Test EventSource groupby return correct object
    """
    grouped = event_source.groupby(by=by)
    assert isinstance(grouped, EventSourceGroupBy)
