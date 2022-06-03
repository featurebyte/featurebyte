"""
Unit test for EventView class
"""
import pytest

from featurebyte.core.event_view import EventView
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


def test_event_view__table_key_not_found(session):
    """
    Test EventView creation when table key not found
    """
    with pytest.raises(KeyError) as exc:
        EventView.from_session(
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
def test_event_view__column_not_found(
    session, timestamp_column, entity_identifiers, expected_column
):
    """
    Test EventView creation when timestamp column not found
    """
    with pytest.raises(KeyError) as exc:
        EventView.from_session(
            session=session,
            table_name='"trans"',
            timestamp_column=timestamp_column,
            entity_identifiers=entity_identifiers,
        )
    expected_msg = f'Could not find the "{expected_column}" column from the table "trans"!'
    assert expected_msg in str(exc.value)


def test_getitem__str(event_view):
    """
    Test retrieving single column
    """
    cust_id = event_view["cust_id"]
    assert isinstance(cust_id, Series)
    assert cust_id.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["cust_id"]},
        output_type=NodeOutputType.SERIES,
    )
    assert cust_id.lineage == ("input_1", "project_1")
    assert cust_id.row_index_lineage == ("input_1",)


def test_getitem__list_of_str(event_view):
    """
    Test retrieving subset of the event source features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_view_subset1 = event_view[["value"]]
    assert isinstance(event_view_subset1, EventView)
    assert set(event_view_subset1.column_var_type_map) == {"created_at", "cust_id", "value"}
    assert event_view_subset1.row_index_lineage == event_view.row_index_lineage
    assert event_view_subset1.inception_node == event_view.inception_node

    # case 2: select a non-protected column with a timestamp column
    event_view_subset2 = event_view[["value", "created_at"]]
    assert isinstance(event_view_subset2, EventView)
    assert set(event_view_subset2.column_var_type_map) == {"created_at", "cust_id", "value"}
    assert event_view_subset2.row_index_lineage == event_view.row_index_lineage
    assert event_view_subset2.inception_node == event_view.inception_node

    # both event source subsets actually point to the same node
    assert event_view_subset1.node == event_view_subset2.node


def test_getitem__series_key(event_view):
    """
    Test filtering on event source object
    """
    mask_cust_id = event_view["cust_id"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.var_type == DBVarType.BOOL

    event_view_row_subset = event_view[mask_cust_id]
    assert isinstance(event_view_row_subset, EventView)
    assert event_view_row_subset.row_index_lineage == ("input_1", "filter_1")
    assert event_view_row_subset.inception_node == event_view.inception_node


@pytest.mark.parametrize("column", ["created_at", "cust_id"])
def test_setitem__override_protected_column(event_view, column):
    """
    Test attempting to change event source's timestamp value or entity identifier value
    """
    assert column in event_view.protected_columns
    with pytest.raises(ValueError) as exc:
        event_view[column] = 1
    expected_msg = f"Timestamp or entity identifier column '{column}' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_setitem__str_key_series_value(event_view):
    """
    Test assigning Series object to event_view
    """
    double_value = event_view["value"] * 2
    assert isinstance(double_value, Series)
    event_view["double_value"] = double_value
    assert event_view.node == Node(
        name="assign_1",
        type=NodeType.ASSIGN,
        parameters={"name": "double_value"},
        output_type=NodeOutputType.FRAME,
    )
    assert event_view.column_lineage_map == {
        "cust_id": ("input_1",),
        "session_id": ("input_1",),
        "event_type": ("input_1",),
        "value": ("input_1",),
        "created_at": ("input_1",),
        "double_value": ("assign_1",),
    }
