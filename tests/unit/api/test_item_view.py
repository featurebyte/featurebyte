"""
Unit test for ItemView class
"""
import pytest

from featurebyte.api.item_view import ItemView
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.fixture(name="snowflake_item_view")
def snowflake_item_view_fixture(snowflake_item_data):
    """
    ItemView fixture
    """
    item_view = ItemView.from_item_data(snowflake_item_data)
    yield item_view


def test_from_item_data(snowflake_item_data):
    """
    Test from_item_data
    """
    with pytest.raises(TypeError) as exc:
        ItemView.from_item_data("hello")
    expected_msg = (
        'type of argument "item_data" must be featurebyte.api.item_data.ItemData; got str instead'
    )
    assert expected_msg in str(exc.value)

    item_view_first = ItemView.from_item_data(snowflake_item_data)
    assert item_view_first.tabular_source == snowflake_item_data.tabular_source
    assert item_view_first.node == snowflake_item_data.node
    assert item_view_first.row_index_lineage == snowflake_item_data.row_index_lineage
    assert item_view_first.columns_info == snowflake_item_data.columns_info


def test_getitem__str(snowflake_item_view):
    """
    Test retrieving single column
    """
    cust_id = snowflake_item_view["item_id_col"]
    assert isinstance(cust_id, Series)
    assert cust_id.node.dict(exclude={"name": True}) == {
        "type": NodeType.PROJECT,
        "parameters": {"columns": ["item_id_col"]},
        "output_type": NodeOutputType.SERIES,
    }
    assert cust_id.row_index_lineage == (snowflake_item_view.node.name,)
    assert cust_id.parent.node == snowflake_item_view.node


def test_getitem__series_key(snowflake_item_view):
    """
    Test filtering on ItemData object
    """
    mask_cust_id = snowflake_item_view["item_amount"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.dtype == DBVarType.BOOL

    row_subset = snowflake_item_view[mask_cust_id]
    assert isinstance(row_subset, ItemView)
    assert row_subset.row_index_lineage == (
        snowflake_item_view.row_index_lineage + (row_subset.node.name,)
    )
    assert row_subset.event_id_column == snowflake_item_view.event_id_column
    assert row_subset.item_id_column == snowflake_item_view.item_id_column
    assert row_subset.event_data_id == snowflake_item_view.event_data_id
    assert row_subset.event_view.dict() == snowflake_item_view.event_view.dict()


@pytest.mark.parametrize("column", ["event_id_col", "item_id_col"])
def test_setitem__override_protected_column(snowflake_item_view, column):
    """
    Test attempting to change ItemData's protected columns
    """
    assert column in snowflake_item_view.protected_columns
    with pytest.raises(ValueError) as exc:
        snowflake_item_view[column] = 1
    expected_msg = f"Column '{column}' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_setitem__str_key_series_value(snowflake_item_view):
    """
    Test assigning Series object to event_view
    """
    source_node_name = snowflake_item_view.node.name
    double_value = snowflake_item_view["item_amount"] * 2
    assert isinstance(double_value, Series)
    snowflake_item_view["double_value"] = double_value
    assert snowflake_item_view.node.dict(exclude={"name": True}) == {
        "type": NodeType.ASSIGN,
        "parameters": {"name": "double_value", "value": None},
        "output_type": NodeOutputType.FRAME,
    }
    assert snowflake_item_view.column_lineage_map == {
        "event_id_col": (source_node_name,),
        "item_id_col": (source_node_name,),
        "item_type": (source_node_name,),
        "item_amount": (source_node_name,),
        "created_at": (source_node_name,),
        "double_value": (snowflake_item_view.node.name,),
    }


def test_unary_op_params(snowflake_item_view):
    """
    Test unary operation inherits tabular_data_ids
    """
    column = snowflake_item_view["item_amount"]
    output = column.isnull()
    assert output.tabular_data_ids == column.tabular_data_ids


def test_event_view_column_getitem_series(snowflake_item_view):
    """
    Test ItemViewColumn filter by boolean mask
    """
    column = snowflake_item_view["item_amount"]
    mask = snowflake_item_view["item_amount"] > 10
    output = column[mask]
    assert output.tabular_data_ids == column.tabular_data_ids
    assert output.name == column.name
    assert output.dtype == column.dtype
    output_dict = output.dict()
    assert output_dict["node_name"] == "filter_1"
    filter_node = next(node for node in output_dict["graph"]["nodes"] if node["name"] == "filter_1")
    assert filter_node == {
        "name": "filter_1",
        "type": "filter",
        "parameters": {},
        "output_type": "series",
    }
    assert output_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "gt_1"},
        {"source": "project_1", "target": "filter_1"},
        {"source": "gt_1", "target": "filter_1"},
    ]
