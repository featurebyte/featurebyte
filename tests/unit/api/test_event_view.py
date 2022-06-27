"""
Unit test for EventView class
"""
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


def test_from_event_data__column_not_found(snowflake_event_data):
    """
    Test EventData from_tabular_source trigger column not found error
    """
    with pytest.raises(ValueError) as exc:
        EventView.from_event_data(
            event_data=snowflake_event_data, entity_identifiers=["unknown_column"]
        )
    assert 'Column "unknown_column" not found in the table!' in str(exc.value)


def test_getitem__str(snowflake_event_view):
    """
    Test retrieving single column
    """
    cust_id = snowflake_event_view["cust_id"]
    assert isinstance(cust_id, Series)
    assert cust_id.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["cust_id"]},
        output_type=NodeOutputType.SERIES,
    )
    assert cust_id.lineage == ("input_2", "project_1")
    assert cust_id.row_index_lineage == ("input_2",)


def test_getitem__list_of_str(snowflake_event_view):
    """
    Test retrieving subset of the event source features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_view_subset1 = snowflake_event_view[["col_float"]]
    assert isinstance(event_view_subset1, EventView)
    assert set(event_view_subset1.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset1.row_index_lineage == snowflake_event_view.row_index_lineage
    assert event_view_subset1.inception_node == snowflake_event_view.inception_node

    # case 2: select a non-protected column with a timestamp column
    event_view_subset2 = snowflake_event_view[["col_float", "event_timestamp"]]
    assert isinstance(event_view_subset2, EventView)
    assert set(event_view_subset2.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset2.row_index_lineage == snowflake_event_view.row_index_lineage
    assert event_view_subset2.inception_node == snowflake_event_view.inception_node

    # both event source subsets actually point to the same node
    assert event_view_subset1.node == event_view_subset2.node


def test_getitem__series_key(snowflake_event_view):
    """
    Test filtering on event source object
    """
    mask_cust_id = snowflake_event_view["cust_id"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.var_type == DBVarType.BOOL

    event_view_row_subset = snowflake_event_view[mask_cust_id]
    assert isinstance(event_view_row_subset, EventView)
    assert event_view_row_subset.row_index_lineage == ("input_2", "filter_1")
    assert event_view_row_subset.inception_node == snowflake_event_view.inception_node


@pytest.mark.parametrize("column", ["event_timestamp"])
def test_setitem__override_protected_column(snowflake_event_view, column):
    """
    Test attempting to change event source's timestamp value or entity identifier value
    """
    assert column in snowflake_event_view.protected_columns
    with pytest.raises(ValueError) as exc:
        snowflake_event_view[column] = 1
    expected_msg = f"Timestamp or entity identifier column '{column}' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_setitem__str_key_series_value(snowflake_event_view):
    """
    Test assigning Series object to event_view
    """
    double_value = snowflake_event_view["col_float"] * 2
    assert isinstance(double_value, Series)
    snowflake_event_view["double_value"] = double_value
    assert snowflake_event_view.node == Node(
        name="assign_1",
        type=NodeType.ASSIGN,
        parameters={"name": "double_value"},
        output_type=NodeOutputType.FRAME,
    )
    assert snowflake_event_view.column_lineage_map == {
        "col_binary": ("input_2",),
        "col_boolean": ("input_2",),
        "col_char": ("input_2",),
        "col_float": ("input_2",),
        "col_int": ("input_2",),
        "col_text": ("input_2",),
        "event_timestamp": ("input_2",),
        "created_at": ("input_2",),
        "cust_id": ("input_2",),
        "double_value": ("assign_1",),
    }
