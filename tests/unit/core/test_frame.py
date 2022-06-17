"""
Unit test for Frame
"""
import pytest

from featurebyte.core.frame import Frame, Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


@pytest.mark.parametrize(
    "item,expected_type",
    [
        ("CUST_ID", DBVarType.INT),
        ("PRODUCT_ACTION", DBVarType.VARCHAR),
        ("VALUE", DBVarType.FLOAT),
        ("MASK", DBVarType.BOOL),
    ],
)
def test__getitem__str_key(dataframe, item, expected_type):
    """
    Test single column retrieval
    """
    series = dataframe[item]
    assert isinstance(series, Series)
    assert series.name == item
    assert series.var_type == expected_type
    assert series.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": [item]},
        output_type=NodeOutputType.SERIES,
    )
    assert series.lineage == ("input_1", "project_1")
    assert series.row_index_lineage == ("input_1",)
    assert dict(series.graph.edges) == {"input_1": ["project_1"]}


def test__getitem__str_not_found(dataframe):
    """
    Test single column retrieval failure
    """
    with pytest.raises(KeyError) as exc:
        _ = dataframe["random"]
    assert "Column random not found!" in str(exc.value)


def test__getitem__list_of_str_key(dataframe):
    """
    Test list of columns retrieval
    """
    sub_dataframe = dataframe[["CUST_ID", "VALUE"]]
    assert isinstance(sub_dataframe, Frame)
    assert sub_dataframe.column_var_type_map == {"CUST_ID": DBVarType.INT, "VALUE": DBVarType.FLOAT}
    assert sub_dataframe.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["CUST_ID", "VALUE"]},
        output_type=NodeOutputType.FRAME,
    )
    assert sub_dataframe.column_lineage_map == {
        "CUST_ID": ("input_1", "project_1"),
        "VALUE": ("input_1", "project_1"),
    }
    assert sub_dataframe.row_index_lineage == ("input_1",)
    assert dict(sub_dataframe.graph.edges) == {"input_1": ["project_1"]}


def test__getitem__list_of_str_not_found(dataframe):
    """
    Test list of columns retrieval failure
    """
    with pytest.raises(KeyError) as exc:
        _ = dataframe[["CUST_ID", "random"]]
    assert "Columns ['random'] not found!" in str(exc.value)


def test__getitem__series_key(dataframe, bool_series):
    """
    Test filtering using boolean Series
    """
    sub_dataframe = dataframe[bool_series]
    assert isinstance(sub_dataframe, Frame)
    assert sub_dataframe.column_var_type_map == dataframe.column_var_type_map
    assert sub_dataframe.node == Node(
        name="filter_1",
        type=NodeType.FILTER,
        parameters={},
        output_type=NodeOutputType.FRAME,
    )
    assert sub_dataframe.column_lineage_map == {
        "CUST_ID": ("input_1", "filter_1"),
        "PRODUCT_ACTION": ("input_1", "filter_1"),
        "VALUE": ("input_1", "filter_1"),
        "MASK": ("input_1", "filter_1"),
    }
    assert sub_dataframe.row_index_lineage == ("input_1", "filter_1")
    assert dict(sub_dataframe.graph.edges) == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["filter_1"],
    }


def test__getitem__non_boolean_series_type_not_supported(dataframe, int_series):
    """
    Test filtering using non-boolean Series
    """
    with pytest.raises(TypeError) as exc:
        _ = dataframe[int_series]
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test__getitem__series_type_row_index_not_aligned(dataframe, bool_series):
    """
    Test filtering using boolean series with different row index
    """
    filtered_bool_series = dataframe[bool_series]["MASK"]
    with pytest.raises(ValueError) as exc:
        _ = dataframe[filtered_bool_series]
    expected_msg = (
        "Row indices between 'Frame(node.name=input_1)' and "
        "'Series[BOOL](name=MASK, node.name=project_2)' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__getitem__type_not_supported(dataframe):
    """
    Test retrieval with non-supported type
    """
    with pytest.raises(TypeError) as exc:
        _ = dataframe[True]
    assert "Frame indexing with value 'True' not supported!" in str(exc.value)


@pytest.mark.parametrize(
    "key,value,expected_type,expected_column_count",
    [
        ("PRODUCT_ACTION", 1, DBVarType.INT, 4),
        ("VALUE", 1.23, DBVarType.FLOAT, 4),
        ("string_col", "random_string", DBVarType.VARCHAR, 5),
        ("bool_col", True, DBVarType.BOOL, 5),
    ],
)
def test__setitem__str_key_scalar_value(
    dataframe, key, value, expected_type, expected_column_count
):
    """
    Test scalar value assignment
    """
    dataframe[key] = value
    assert dataframe.column_var_type_map[key] == expected_type
    assert dataframe.node == Node(
        name="assign_1",
        type="assign",
        parameters={"value": value, "name": key},
        output_type=NodeOutputType.FRAME,
    )
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dict(dataframe.graph.edges) == {"input_1": ["assign_1"]}


@pytest.mark.parametrize(
    "key,value_key,expected_type,expected_column_count",
    [
        ("random", "CUST_ID", DBVarType.INT, 5),
        ("CUST_ID", "PRODUCT_ACTION", DBVarType.VARCHAR, 4),
        ("random", "VALUE", DBVarType.FLOAT, 5),
        ("PRODUCT_ACTION", "MASK", DBVarType.BOOL, 4),
    ],
)
def test__setitem__str_key_series_value(
    dataframe, key, value_key, expected_type, expected_column_count
):
    """
    Test Series object value assignment
    """
    value = dataframe[value_key]
    assert isinstance(value, Series)
    dataframe[key] = value
    assert dataframe.column_var_type_map[key] == expected_type
    assert dataframe.node == Node(
        name="assign_1",
        type="assign",
        parameters={"name": key},
        output_type=NodeOutputType.FRAME,
    )
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dict(dataframe.graph.edges) == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["assign_1"],
    }


def test__setitem__str_key_series_value__row_index_not_aligned(dataframe, bool_series):
    """
    Test Series object value assignment with different row index lineage
    """
    value = dataframe[bool_series]["PRODUCT_ACTION"]
    assert isinstance(value, Series)
    assert value.lineage == ("input_1", "filter_1", "project_2")
    assert value.row_index_lineage == ("input_1", "filter_1")
    with pytest.raises(ValueError) as exc:
        dataframe["new_column"] = value
    expected_msg = (
        "Row indices between 'Frame(node.name=input_1)' and "
        "'Series[VARCHAR](name=PRODUCT_ACTION, node.name=project_2)' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__setitem__type_not_supported(dataframe):
    """
    Test assignment with non-supported key type
    """
    with pytest.raises(TypeError) as exc:
        dataframe[1.234] = True
    assert "Setting key '1.234' with value 'True' not supported!" in str(exc.value)


def test_multiple_statements(dataframe):
    """
    Test multiple statements
    """
    dataframe = dataframe[dataframe["MASK"]]
    cust_id = dataframe["CUST_ID"]
    dataframe["amount"] = cust_id + dataframe["VALUE"]
    dataframe["vip_customer"] = (dataframe["CUST_ID"] < 1000) & (dataframe["amount"] > 1000.0)

    assert cust_id.name == "CUST_ID"
    assert cust_id.node == Node(
        name="project_2",
        type=NodeType.PROJECT,
        parameters={"columns": ["CUST_ID"]},
        output_type=NodeOutputType.SERIES,
    )
    assert cust_id.lineage == ("input_1", "filter_1", "project_2")
    assert cust_id.row_index_lineage == ("input_1", "filter_1")
    assert dataframe.column_var_type_map == {
        "CUST_ID": DBVarType.INT,
        "PRODUCT_ACTION": DBVarType.VARCHAR,
        "VALUE": DBVarType.FLOAT,
        "MASK": DBVarType.BOOL,
        "amount": DBVarType.FLOAT,
        "vip_customer": DBVarType.BOOL,
    }
    assert dataframe.columns == [
        "CUST_ID",
        "PRODUCT_ACTION",
        "VALUE",
        "MASK",
        "amount",
        "vip_customer",
    ]
    assert dataframe.node == Node(
        name="assign_2",
        type=NodeType.ASSIGN,
        parameters={"name": "vip_customer"},
        output_type=NodeOutputType.FRAME,
    )
    assert dataframe.column_lineage_map == {
        "CUST_ID": ("input_1", "filter_1"),
        "PRODUCT_ACTION": ("input_1", "filter_1"),
        "VALUE": ("input_1", "filter_1"),
        "MASK": ("input_1", "filter_1"),
        "amount": ("assign_1",),
        "vip_customer": ("assign_2",),
    }
    assert dataframe.row_index_lineage == ("input_1", "filter_1")
    assert dict(dataframe.graph.edges) == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["filter_1"],
        "filter_1": ["project_2", "project_3", "assign_1"],
        "project_2": ["add_1", "lt_1"],
        "project_3": ["add_1"],
        "add_1": ["assign_1"],
        "assign_1": ["project_4", "assign_2"],
        "project_4": ["gt_1"],
        "lt_1": ["and_1"],
        "gt_1": ["and_1"],
        "and_1": ["assign_2"],
    }


def test_frame_column_order(dataframe):
    """
    Check columns are sorted by added order
    """
    original_columns = ["CUST_ID", "PRODUCT_ACTION", "VALUE", "MASK"]
    assert dataframe.columns == original_columns
    dataframe["first_added_column"] = dataframe["CUST_ID"] * 10
    assert dataframe.columns == original_columns + ["first_added_column"]
    dataframe["second_added_column"] = dataframe["VALUE"] * dataframe["CUST_ID"]
    assert dataframe.columns == original_columns + ["first_added_column", "second_added_column"]
    dataframe["third_added_column"] = dataframe["VALUE"] + 1.234
    assert dataframe.columns == original_columns + [
        "first_added_column",
        "second_added_column",
        "third_added_column",
    ]
