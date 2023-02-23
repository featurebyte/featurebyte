"""
Unit test for Frame
"""
import pytest

from featurebyte.core.frame import Frame, Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import get_node


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
    series_dict = series.dict()
    assert series_dict["name"] == item
    assert series_dict["dtype"] == expected_type
    assert series_dict["node_name"] == "project_1"
    node = get_node(series_dict["graph"], "project_1")
    assert node == {
        "name": "project_1",
        "type": NodeType.PROJECT,
        "parameters": {"columns": [item]},
        "output_type": NodeOutputType.SERIES,
    }
    assert series_dict["graph"]["edges"] == [{"source": "input_1", "target": "project_1"}]


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
    sub_dataframe_dict = sub_dataframe.dict()
    assert sub_dataframe_dict["columns_info"] == [
        {
            "name": "CUST_ID",
            "dtype": DBVarType.INT,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "VALUE",
            "dtype": DBVarType.FLOAT,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
    ]
    assert sub_dataframe_dict["node_name"] == "project_1"
    node = get_node(sub_dataframe_dict["graph"], "project_1")
    assert node == {
        "name": "project_1",
        "type": NodeType.PROJECT,
        "parameters": {"columns": ["CUST_ID", "VALUE"]},
        "output_type": NodeOutputType.FRAME,
    }
    assert sub_dataframe_dict["graph"]["edges"] == [{"source": "input_1", "target": "project_1"}]


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
    assert sub_dataframe.columns_info == dataframe.columns_info
    assert isinstance(sub_dataframe, Frame)
    assert sub_dataframe.row_index_lineage == ("input_1", "filter_1")
    sub_dataframe_dict = sub_dataframe.dict()
    assert sub_dataframe_dict["node_name"] == "filter_1"
    node = get_node(sub_dataframe_dict["graph"], "filter_1")
    assert node == {
        "name": "filter_1",
        "type": NodeType.FILTER,
        "parameters": {},
        "output_type": NodeOutputType.FRAME,
    }
    assert sub_dataframe_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "filter_1"},
        {"source": "project_1", "target": "filter_1"},
    ]


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
        f"Row indices between 'Frame(node_name={dataframe.node_name})' and "
        f"'Series[BOOL](name=MASK, node_name={filtered_bool_series.node_name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__getitem__type_not_supported(dataframe):
    """
    Test retrieval with non-supported type
    """
    with pytest.raises(TypeError) as exc:
        _ = dataframe[True]
    expected_msg = (
        'type of argument "item" must be one of (str, List[str], featurebyte.core.series.Series); '
        "got bool instead"
    )
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize(
    "key,value,expected_type,expected_column_count",
    [
        ("PRODUCT_ACTION", 1, DBVarType.INT, 6),
        ("VALUE", 1.23, DBVarType.FLOAT, 6),
        ("string_col", "random_string", DBVarType.VARCHAR, 7),
        ("bool_col", True, DBVarType.BOOL, 7),
    ],
)
def test__setitem__str_key_scalar_value(
    dataframe, key, value, expected_type, expected_column_count
):
    """
    Test scalar value assignment
    """
    dataframe[key] = value
    dataframe_dict = dataframe.dict()
    assert dataframe.column_var_type_map[key] == expected_type
    assert dataframe_dict["node_name"] == "assign_1"
    node = get_node(dataframe_dict["graph"], "assign_1")
    assert node == {
        "name": "assign_1",
        "type": "assign",
        "parameters": {"value": value, "name": key},
        "output_type": NodeOutputType.FRAME,
    }
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dataframe_dict["graph"]["edges"] == [{"source": "input_1", "target": "assign_1"}]


def test__setitem__tuple_assignment_with_mask(dataframe):
    """
    Test tuple assignment
    """
    mask = dataframe["MASK"]
    key_to_use = "VALUE"
    dataframe[mask, key_to_use] = 1
    dataframe_dict = dataframe.dict()
    node = get_node(dataframe_dict["graph"], "conditional_1")
    assert node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 1},
        "output_type": NodeOutputType.SERIES,
    }
    assert len(dataframe.column_var_type_map.keys()) == 6
    assert dataframe_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "project_2", "target": "conditional_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "conditional_1", "target": "assign_1"},
    ]


@pytest.mark.parametrize(
    "key,value_key,expected_type,expected_column_count",
    [
        ("random", "CUST_ID", DBVarType.INT, 7),
        ("CUST_ID", "PRODUCT_ACTION", DBVarType.VARCHAR, 6),
        ("random", "VALUE", DBVarType.FLOAT, 7),
        ("PRODUCT_ACTION", "MASK", DBVarType.BOOL, 6),
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
    dataframe_dict = dataframe.dict()
    assert dataframe.column_var_type_map[key] == expected_type
    assert dataframe_dict["node_name"] == "assign_1"
    node = get_node(dataframe_dict["graph"], "assign_1")
    assert node == {
        "name": "assign_1",
        "type": "assign",
        "parameters": {"name": key, "value": None},
        "output_type": NodeOutputType.FRAME,
    }
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dataframe_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "project_1", "target": "assign_1"},
    ]


def test__setitem__str_key_series_value__row_index_not_aligned(dataframe, bool_series):
    """
    Test Series object value assignment with different row index lineage
    """
    value = dataframe[bool_series]["PRODUCT_ACTION"]
    assert value.row_index_lineage == ("input_1", "filter_1")
    assert isinstance(value, Series)
    with pytest.raises(ValueError) as exc:
        dataframe["new_column"] = value
    expected_msg = (
        f"Row indices between 'Frame(node_name={dataframe.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node_name={value.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__setitem__type_not_supported(dataframe):
    """
    Test assignment with non-supported key type
    """
    with pytest.raises(TypeError) as exc:
        dataframe[1.234] = True
    assert (
        'type of argument "key" must be one of (str, Tuple[featurebyte.core.series.Series, str]); got float '
        "instead" in str(exc.value)
    )


def test__set_item__assigning_feature_not_supported(dataframe, production_ready_feature):
    """
    Test that applying a feature mask with a column name on a frame fails. We should only allow a Series mask to be
    applied.
    """
    feature_mask = production_ready_feature == 14.0

    dataframe["amount"] = dataframe["VALUE"]
    with pytest.raises(ValueError) as exc:
        dataframe[feature_mask, "amount"] = 100
    assert "The mask provided should be a Series" in str(exc)


def test_multiple_statements(dataframe):
    """
    Test multiple statements
    """
    dataframe = dataframe[dataframe["MASK"]]
    cust_id = dataframe["CUST_ID"]
    dataframe["amount"] = cust_id + dataframe["VALUE"]
    dataframe["vip_customer"] = (dataframe["CUST_ID"] < 1000) & (dataframe["amount"] > 1000.0)

    cust_id_dict = cust_id.dict()
    dataframe_dict = dataframe.dict()

    assert cust_id_dict["name"] == "CUST_ID"
    assert cust_id_dict["node_name"] == "project_2"
    node = get_node(cust_id_dict["graph"], "project_2")
    assert node == {
        "name": "project_2",
        "type": NodeType.PROJECT,
        "parameters": {"columns": ["CUST_ID"]},
        "output_type": NodeOutputType.SERIES,
    }
    assert cust_id.row_index_lineage == ("input_1", "filter_1")
    assert dataframe_dict["columns_info"] == [
        {
            "name": "CUST_ID",
            "dtype": DBVarType.INT,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "PRODUCT_ACTION",
            "dtype": DBVarType.VARCHAR,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "VALUE",
            "dtype": DBVarType.FLOAT,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "MASK",
            "dtype": DBVarType.BOOL,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "TIMESTAMP",
            "dtype": DBVarType.TIMESTAMP,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "PROMOTION_START_DATE",
            "dtype": DBVarType.DATE,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "amount",
            "dtype": DBVarType.FLOAT,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "vip_customer",
            "dtype": DBVarType.BOOL,
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
    ]
    assert dataframe.columns == [
        "CUST_ID",
        "PRODUCT_ACTION",
        "VALUE",
        "MASK",
        "TIMESTAMP",
        "PROMOTION_START_DATE",
        "amount",
        "vip_customer",
    ]
    assert dataframe_dict["node_name"] == "assign_2"
    node = get_node(dataframe_dict["graph"], "assign_2")
    assert node == {
        "name": "assign_2",
        "type": NodeType.ASSIGN,
        "parameters": {"name": "vip_customer", "value": None},
        "output_type": NodeOutputType.FRAME,
    }
    assert dataframe.row_index_lineage == ("input_1", "filter_1")
    assert dataframe_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "filter_1"},
        {"source": "project_1", "target": "filter_1"},
        {"source": "filter_1", "target": "project_2"},
        {"source": "filter_1", "target": "project_3"},
        {"source": "project_2", "target": "add_1"},
        {"source": "project_3", "target": "add_1"},
        {"source": "filter_1", "target": "assign_1"},
        {"source": "add_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_4"},
        {"source": "project_4", "target": "lt_1"},
        {"source": "assign_1", "target": "project_5"},
        {"source": "project_5", "target": "gt_1"},
        {"source": "lt_1", "target": "and_1"},
        {"source": "gt_1", "target": "and_1"},
        {"source": "assign_1", "target": "assign_2"},
        {"source": "and_1", "target": "assign_2"},
    ]


def test_frame_column_order(dataframe):
    """
    Check columns are sorted by added order
    """
    original_columns = [
        "CUST_ID",
        "PRODUCT_ACTION",
        "VALUE",
        "MASK",
        "TIMESTAMP",
        "PROMOTION_START_DATE",
    ]
    assert dataframe.columns == original_columns
    dataframe["first_added_column"] = dataframe.CUST_ID * 10
    assert dataframe.columns == original_columns + ["first_added_column"]
    dataframe["second_added_column"] = dataframe.VALUE * dataframe.CUST_ID
    assert dataframe.columns == original_columns + ["first_added_column", "second_added_column"]
    dataframe["third_added_column"] = dataframe.VALUE + 1.234
    assert dataframe.columns == original_columns + [
        "first_added_column",
        "second_added_column",
        "third_added_column",
    ]


def test_frame__dict(dataframe):
    """
    Test frame serialization to dictionary format
    """
    dataframe["feat"] = dataframe["VALUE"] * dataframe["CUST_ID"]
    unused_dataframe = dataframe[dataframe["VALUE"] > 100.0]
    filtered_dataframe = dataframe[dataframe["MASK"]]
    sub_dataframe = filtered_dataframe[["VALUE", "CUST_ID"]]
    sub_dataframe_dict = sub_dataframe.dict()
    assert isinstance(unused_dataframe, Frame)
    assert sub_dataframe.row_index_lineage == ("input_1", "filter_2")
    sub_dataframe_dict["feature_store"] = dataframe.feature_store
    loaded_sub_dataframe = Frame.parse_obj(sub_dataframe_dict)
    # note that loaded_sub_dataframe & sub_dataframe are not fully identical (ideally, we should make them identical)
    # loaded_sub_dataframe = input -> filter (input_frame, "MASK") -> project ["VALUE", "CUST_ID"]
    # sub_dataframe = input -> filter (assign_frame, "MASK") -> project ["VALUE", "CUST_ID"]
    assert sub_dataframe.preview_sql() == loaded_sub_dataframe.preview_sql()


def test_frame_copy(dataframe):
    """
    Test dataframe copy
    """
    assert dataframe.feature_store is not None
    new_dataframe = dataframe.copy()
    assert new_dataframe.feature_store == dataframe.feature_store
    assert id(new_dataframe.graph.nodes) == id(dataframe.graph.nodes)

    deep_dataframe = dataframe.copy(deep=True)
    assert deep_dataframe == dataframe
    assert id(deep_dataframe.graph.nodes) == id(dataframe.graph.nodes)


def test_frame__getattr__method(dataframe):
    """
    Test Frame __getattr__ magic method
    """
    assert dataframe.CUST_ID == dataframe["CUST_ID"]
    with pytest.raises(AttributeError):
        # expect to throw attribute error rather than KeyError due to column not exists
        _ = dataframe.random_attribute

    # check that "columns" column is set properly
    dataframe["columns"] = dataframe["CUST_ID"] + 1
    assert isinstance(dataframe["columns"], Series)

    # when accessing the `columns` attribute, make sure we don't retrieve a Series object
    assert set(dataframe.columns) == {
        "CUST_ID",
        "PRODUCT_ACTION",
        "VALUE",
        "TIMESTAMP",
        "MASK",
        "PROMOTION_START_DATE",
        "columns",
    }


def test_frame__autocompletion(dataframe):
    """
    Test Frame __dir__ and _ipython_key_completions_ methods
    """
    assert set(dataframe.columns).issubset(dir(dataframe))
    assert dataframe._ipython_key_completions_() == set(dataframe.columns)


def test_frame__project_always_uses_current_node_as_input(dataframe):
    """
    Test project node always uses the current node as input
    """
    sub_df = dataframe[["CUST_ID", "VALUE"]]
    cust_id = sub_df["CUST_ID"]

    # check the parent node of the cust_id points to the input node but not project node
    parent_nodes = sub_df.graph.backward_edges_map[cust_id.node_name]
    assert len(parent_nodes) == 1
    node = sub_df.graph.get_node_by_name(parent_nodes[0])
    assert node.type == NodeType.PROJECT

    # check the pruned graph (expected to be improved in the future - project_1 should be pruned
    # automatically since it is redundant)
    graph = cust_id.dict()["graph"]
    assert graph["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "project_2"},
    ]
    assert graph["nodes"][2] == {
        "name": "project_2",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["CUST_ID"]},
    }
