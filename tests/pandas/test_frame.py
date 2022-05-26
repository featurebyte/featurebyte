"""
Unit test for DataFrame
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.pandas.frame import DataFrame, Series
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


@pytest.fixture(name="graph")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(name="dataframe")
def dataframe_fixture(graph):
    """
    DataFrame test fixture
    """
    column_var_type_map = {
        "CUST_ID": DBVarType.INT,
        "PRODUCT_ACTION": DBVarType.VARCHAR,
        "VALUE": DBVarType.FLOAT,
        "MASK": DBVarType.BOOL,
    }
    node = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return DataFrame(
        node=node, column_var_type_map=column_var_type_map, row_index_lineage=[node.name]
    )


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
    assert isinstance(sub_dataframe, DataFrame)
    assert sub_dataframe.column_var_type_map == {"CUST_ID": DBVarType.INT, "VALUE": DBVarType.FLOAT}
    assert sub_dataframe.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["CUST_ID", "VALUE"]},
        output_type=NodeOutputType.FRAME,
    )
    assert sub_dataframe.row_index_lineage == ("input_1",)
    assert dict(sub_dataframe.graph.edges) == {"input_1": ["project_1"]}


def test__getitem__list_of_str_not_found(dataframe):
    """
    Test list of columns retrieval failure
    """
    with pytest.raises(KeyError) as exc:
        _ = dataframe[["CUST_ID", "random"]]
    assert "Columns ['random'] not found!" in str(exc.value)


def test__getitem__series(dataframe):
    """
    Test filtering using boolean Series
    """
    sub_dataframe = dataframe[dataframe["MASK"]]
    assert isinstance(sub_dataframe, DataFrame)
    assert sub_dataframe.column_var_type_map == dataframe.column_var_type_map
    assert sub_dataframe.node == Node(
        name="filter_1",
        type=NodeType.FILTER,
        parameters={},
        output_type=NodeOutputType.FRAME,
    )
    assert sub_dataframe.row_index_lineage == ("input_1", "filter_1")
    assert dict(sub_dataframe.graph.edges) == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["filter_1"],
    }


def test__getitem__series_type_not_supported(dataframe):
    """
    Test filtering using non-boolean Series
    """
    non_boolean_series = dataframe["CUST_ID"]
    with pytest.raises(TypeError) as exc:
        _ = dataframe[non_boolean_series]
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test__getitem__type_not_supported(dataframe):
    """
    Test retrieval with non-supported type
    """
    with pytest.raises(TypeError) as exc:
        _ = dataframe[True]
    assert "Type <class 'bool'> not supported!" in str(exc.value)


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
        parameters={"value": value},
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
        parameters={},
        output_type=NodeOutputType.FRAME,
    )
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dict(dataframe.graph.edges) == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["assign_1"],
    }


def test__setitem__str_key_series_value__row_index_not_aligned(dataframe):
    """
    Test Series object value assignment with different row index lineage
    """
    mask = dataframe["MASK"]
    value = dataframe[mask]["PRODUCT_ACTION"]
    assert isinstance(value, Series)
    assert value.row_index_lineage == ("input_1", "filter_1")
    with pytest.raises(ValueError) as exc:
        dataframe["new_column"] = value
    assert "Row index not aligned!" in str(exc.value)


def test__setitem__type_not_supported(dataframe):
    """
    Test assignment with non-supported type
    """
    with pytest.raises(TypeError) as exc:
        dataframe[1.234] = True
    expected_msg = "Key type <class 'float'> with value type <class 'bool'> not supported!"
    assert expected_msg in str(exc.value)
