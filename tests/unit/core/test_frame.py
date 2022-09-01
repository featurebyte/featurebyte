"""
Unit test for Frame
"""
import pytest

from featurebyte.core.frame import Frame, Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


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
    assert series_dict["var_type"] == expected_type
    assert (
        series_dict["node"].items()
        >= {
            "type": NodeType.PROJECT,
            "parameters": {"columns": [item]},
            "output_type": NodeOutputType.SERIES,
        }.items()
    )
    assert series_dict["row_index_lineage"] == ("input_1",)
    assert dict(series_dict["graph"]["edges"]) == {"input_1": ["project_1"]}


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
        {"name": "CUST_ID", "var_type": DBVarType.INT, "entity_id": None},
        {"name": "VALUE", "var_type": DBVarType.FLOAT, "entity_id": None},
    ]
    assert (
        sub_dataframe_dict["node"].items()
        >= {
            "type": NodeType.PROJECT,
            "parameters": {"columns": ["CUST_ID", "VALUE"]},
            "output_type": NodeOutputType.FRAME,
        }.items()
    )
    assert sub_dataframe_dict["column_lineage_map"] == {
        "CUST_ID": ("input_1", "project_1"),
        "VALUE": ("input_1", "project_1"),
    }
    assert sub_dataframe_dict["row_index_lineage"] == ("input_1",)
    assert dict(sub_dataframe_dict["graph"]["edges"]) == {"input_1": ["project_1"]}


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
    sub_dataframe_dict = sub_dataframe.dict()
    assert (
        sub_dataframe_dict["node"].items()
        >= {
            "type": NodeType.FILTER,
            "parameters": {},
            "output_type": NodeOutputType.FRAME,
        }.items()
    )
    assert sub_dataframe_dict["column_lineage_map"] == {
        "CUST_ID": ("input_1", "filter_1"),
        "PRODUCT_ACTION": ("input_1", "filter_1"),
        "VALUE": ("input_1", "filter_1"),
        "MASK": ("input_1", "filter_1"),
        "TIMESTAMP": ("input_1", "filter_1"),
        "PROMOTION_START_DATE": ("input_1", "filter_1"),
    }
    assert sub_dataframe_dict["row_index_lineage"] == ("input_1", "filter_1")
    assert dict(sub_dataframe_dict["graph"]["edges"]) == {
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
        f"Row indices between 'Frame(node.name={dataframe.node.name})' and "
        f"'Series[BOOL](name=MASK, node.name={filtered_bool_series.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__getitem__type_not_supported(dataframe):
    """
    Test retrieval with non-supported type
    """
    with pytest.raises(TypeError) as exc:
        _ = dataframe[True]
    expected_msg = 'type of argument "item" must be one of (str, List[str], featurebyte.core.series.Series); got bool instead'
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize(
    "key,value,expected_type,expected_column_count",
    [
        ("PRODUCT_ACTION", 1, DBVarType.INT, 5),
        ("VALUE", 1.23, DBVarType.FLOAT, 5),
        ("string_col", "random_string", DBVarType.VARCHAR, 6),
        ("bool_col", True, DBVarType.BOOL, 6),
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
    assert (
        dataframe_dict["node"].items()
        >= {
            "type": "assign",
            "parameters": {"value": value, "name": key},
            "output_type": NodeOutputType.FRAME,
        }.items()
    )
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dict(dataframe_dict["graph"]["edges"]) == {"input_1": ["assign_1"]}


@pytest.mark.parametrize(
    "key,value_key,expected_type,expected_column_count",
    [
        ("random", "CUST_ID", DBVarType.INT, 6),
        ("CUST_ID", "PRODUCT_ACTION", DBVarType.VARCHAR, 5),
        ("random", "VALUE", DBVarType.FLOAT, 6),
        ("PRODUCT_ACTION", "MASK", DBVarType.BOOL, 5),
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
    assert (
        dataframe_dict["node"].items()
        >= {
            "type": "assign",
            "parameters": {"name": key},
            "output_type": NodeOutputType.FRAME,
        }.items()
    )
    assert len(dataframe.column_var_type_map.keys()) == expected_column_count
    assert dict(dataframe_dict["graph"]["edges"]) == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["assign_1"],
    }


def test__setitem__str_key_series_value__row_index_not_aligned(dataframe, bool_series):
    """
    Test Series object value assignment with different row index lineage
    """
    value = dataframe[bool_series]["PRODUCT_ACTION"]
    assert isinstance(value, Series)
    value_dict = value.dict()
    assert value_dict["row_index_lineage"] == ("input_1", "filter_1")
    with pytest.raises(ValueError) as exc:
        dataframe["new_column"] = value
    expected_msg = (
        f"Row indices between 'Frame(node.name={dataframe.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node.name={value.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__setitem__type_not_supported(dataframe):
    """
    Test assignment with non-supported key type
    """
    with pytest.raises(TypeError) as exc:
        dataframe[1.234] = True
    assert 'type of argument "key" must be str; got float instead' in str(exc.value)


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
    assert (
        cust_id_dict["node"].items()
        >= {
            "type": NodeType.PROJECT,
            "parameters": {"columns": ["CUST_ID"]},
            "output_type": NodeOutputType.SERIES,
        }.items()
    )
    assert cust_id_dict["row_index_lineage"] == ("input_1", "filter_1")
    assert dataframe_dict["columns_info"] == [
        {"name": "CUST_ID", "var_type": DBVarType.INT, "entity_id": None},
        {"name": "PRODUCT_ACTION", "var_type": DBVarType.VARCHAR, "entity_id": None},
        {"name": "VALUE", "var_type": DBVarType.FLOAT, "entity_id": None},
        {"name": "MASK", "var_type": DBVarType.BOOL, "entity_id": None},
        {"name": "TIMESTAMP", "var_type": DBVarType.TIMESTAMP, "entity_id": None},
        {"name": "amount", "var_type": DBVarType.FLOAT, "entity_id": None},
        {"name": "vip_customer", "var_type": DBVarType.BOOL, "entity_id": None},
    ]
    assert dataframe.columns == [
        "CUST_ID",
        "PRODUCT_ACTION",
        "VALUE",
        "MASK",
        "TIMESTAMP",
        "amount",
        "vip_customer",
    ]
    assert (
        dataframe_dict["node"].items()
        >= {
            "type": NodeType.ASSIGN,
            "parameters": {"name": "vip_customer"},
            "output_type": NodeOutputType.FRAME,
        }.items()
    )
    assert dataframe_dict["column_lineage_map"] == {
        "CUST_ID": ("input_1", "filter_1"),
        "PRODUCT_ACTION": ("input_1", "filter_1"),
        "VALUE": ("input_1", "filter_1"),
        "MASK": ("input_1", "filter_1"),
        "TIMESTAMP": ("input_1", "filter_1"),
        "amount": ("assign_1",),
        "vip_customer": ("assign_2",),
    }
    assert dataframe_dict["row_index_lineage"] == ("input_1", "filter_1")
    assert dict(dataframe_dict["graph"]["edges"]) == {
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
    original_columns = ["CUST_ID", "PRODUCT_ACTION", "VALUE", "MASK", "TIMESTAMP"]
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
    assert sub_dataframe_dict["column_lineage_map"] == {
        "CUST_ID": ("input_1", "filter_1", "project_2"),
        "VALUE": ("input_1", "filter_1", "project_2"),
    }
    assert sub_dataframe_dict["column_lineage_map"] == {
        "CUST_ID": ("input_1", "filter_1", "project_2"),
        "VALUE": ("input_1", "filter_1", "project_2"),
    }
    assert sub_dataframe_dict["row_index_lineage"] == ("input_1", "filter_1")
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
