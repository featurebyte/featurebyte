"""
Unit test for Series
"""
import textwrap
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import OperationStructureExtractor
from featurebyte.query_graph.node import construct_node
from tests.util.helper import get_node


def test__getitem__series_key(int_series, bool_series):
    """
    Test filtering using boolean Series
    """
    series = int_series[bool_series]
    assert series.parent is None
    series_dict = series.dict()
    assert series_dict["node_name"] == "filter_1"
    assert series_dict["name"] == int_series.name
    assert series_dict["dtype"] == int_series.dtype
    assert series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_2", "target": "filter_1"},
        {"source": "project_1", "target": "filter_1"},
    ]


def test__getitem__non_boolean_series_type_not_supported(int_series, bool_series):
    """
    Test filtering using non-boolean series
    """
    with pytest.raises(TypeError) as exc:
        _ = bool_series[int_series]
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test__getitem__row_index_not_aligned(int_series, bool_series):
    """
    Test filtering using non-aligned row index
    """
    filtered_bool_series = bool_series[bool_series]
    assert filtered_bool_series.dtype == DBVarType.BOOL
    assert filtered_bool_series.parent is None
    with pytest.raises(ValueError) as exc:
        _ = int_series[filtered_bool_series]
    expected_msg = (
        f"Row indices between 'Series[INT](name=CUST_ID, node_name={int_series.node.name})' and "
        f"'Series[BOOL](name=MASK, node_name={filtered_bool_series.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__getitem__type_not_supported(int_series):
    """
    Test retrieval with unsupported type
    """
    with pytest.raises(TypeError) as exc:
        _ = int_series[True]
    expected_msg = (
        'type of argument "item" must be featurebyte.core.series.FrozenSeries; got bool instead'
    )
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize(
    "column,value",
    [
        ("CUST_ID", 100),
        ("PRODUCT_ACTION", "string_val"),
        ("VALUE", 10),
        ("VALUE", 1.23),
        ("VALUE", float("nan")),
        ("VALUE", np.nan),
        ("CUST_ID", np.nan),
        ("CUST_ID", None),
    ],
)
def test__setitem__bool_series_key_scalar_value(dataframe, bool_series, column, value):
    """
    Test Series conditional assignment
    """
    series = dataframe[column]
    assert series.parent is dataframe
    series[bool_series] = value
    assert series.parent is dataframe
    series_dict = series.dict()
    assert series_dict["node_name"] == "project_3"
    cond_node = get_node(series_dict["graph"], "conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": value},
        "output_type": "series",
    }
    assert dict(series.graph.edges_map) == {
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["conditional_1"],
        "project_2": ["conditional_1"],
        "conditional_1": ["assign_1"],
        "assign_1": ["project_3"],
    }


def test__setitem__int_series_assign_float_value(bool_series, int_series):
    """
    Test series conditional assignment with float value
    """
    assert int_series.dtype == DBVarType.INT
    int_series[bool_series] = 1.1
    assert int_series.dtype == DBVarType.FLOAT
    assert int_series.parent.column_var_type_map[int_series.name] == DBVarType.FLOAT


def test__setitem__cond_assign_with_same_input_nodes(bool_series):
    """
    Test Series conditional assignment using same series for filtering & assignment
    """
    bool_series[bool_series] = True
    assert bool_series.parent is not None
    bool_series_dict = bool_series.dict()
    assert bool_series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "conditional_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_2"},
    ]


def test__setitem__cond_assign_consecutive(dataframe, bool_series):
    """
    Test Series conditional assignment consecutive operations
    """
    series = dataframe["VALUE"]
    series[bool_series] = 100
    series[bool_series] = 200
    series_dict = series.dict()
    cond1_node = get_node(series_dict["graph"], "conditional_1")
    cond2_node = get_node(series_dict["graph"], "conditional_2")
    assert cond1_node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 100},
        "output_type": "series",
    }
    assert cond2_node == {
        "name": "conditional_2",
        "type": "conditional",
        "parameters": {"value": 200},
        "output_type": "series",
    }
    assert series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_2", "target": "conditional_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "conditional_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_3"},
        {"source": "project_3", "target": "conditional_2"},
        {"source": "project_1", "target": "conditional_2"},
        {"source": "assign_1", "target": "assign_2"},
        {"source": "conditional_2", "target": "assign_2"},
        {"source": "assign_2", "target": "project_4"},
    ]


def test__setitem__conditional_assign_series(int_series):
    """
    Test conditional assign with another series
    """
    double_series = int_series * 2
    mask = int_series > 5
    int_series[mask] = double_series[mask]
    int_series_dict = int_series.dict()
    assert int_series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "gt_1"},
        {"source": "project_1", "target": "mul_1"},
        {"source": "mul_1", "target": "filter_1"},
        {"source": "gt_1", "target": "filter_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "gt_1", "target": "conditional_1"},
        {"source": "filter_1", "target": "conditional_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "conditional_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_2"},
    ]


def test__setitem__conditional_assign_unnamed_series(int_series, bool_series):
    """
    Test conditional assign on a temporary series
    """
    temp_series = int_series + 1234
    temp_series[bool_series] = 0
    temp_series_dict = temp_series.dict()
    # Unnamed series stays unnamed (not a PROJECT node)
    assert temp_series_dict["node_name"] == "conditional_1"
    cond_node = get_node(temp_series_dict["graph"], "conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0},
        "type": "conditional",
    }
    # No assignment occurred
    assert temp_series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_2", "target": "add_1"},
        {"source": "add_1", "target": "conditional_1"},
        {"source": "project_1", "target": "conditional_1"},
    ]


def test__setitem__conditional_assign_dictionary_feature(count_per_category_feature):
    """
    Test conditional assign on a dictionary feature
    """
    with pytest.raises(ValueError) as exc:
        count_per_category_feature[count_per_category_feature.isnull()] = 0
    node_name = count_per_category_feature.node_name
    assert str(exc.value) == (
        f"Conditionally updating 'Feature[OBJECT](name=counts_1d, node_name={node_name})' of type"
        f" OBJECT is not supported!"
    )


def test__setitem__row_index_not_aligned(int_series, bool_series):
    """
    Test conditional assignment using non-aligned series
    """
    filtered_bool_series = bool_series[bool_series]
    assert filtered_bool_series.parent is None
    with pytest.raises(ValueError) as exc:
        int_series[filtered_bool_series] = 1
    expected_msg = (
        f"Row indices between 'Series[INT](name=CUST_ID, node_name={int_series.node.name})' and "
        f"'Series[BOOL](name=MASK, node_name={filtered_bool_series.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__setitem__value_type_not_correct(int_series, bool_series):
    """
    Test conditional assignment when the assigned value not valid for given Series type
    """
    with pytest.raises(ValueError) as exc:
        int_series[bool_series] = "abc"
    expected_msg = (
        f"Conditionally updating 'Series[INT](name=CUST_ID, node_name=project_1)' with value 'abc' "
        f"is not supported!"
    )
    assert expected_msg == str(exc.value)


def test__setitem__non_boolean_series_type_not_supported(int_series, bool_series):
    """
    Test conditional assignment using non-boolean series
    """
    with pytest.raises(TypeError) as exc:
        bool_series[int_series] = True
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test__setitem__key_type_not_supported(int_series):
    """
    Test assignment with non-supported key type
    """
    with pytest.raises(TypeError) as exc:
        int_series[1] = True
    expected_msg = (
        'type of argument "key" must be featurebyte.core.series.FrozenSeries; got int instead'
    )
    assert expected_msg in str(exc.value)


def test_logical_operators(bool_series, int_series):
    """
    Test logical operators
    """
    output_and_series = bool_series & bool_series
    assert output_and_series.name is None
    assert output_and_series.parent is None
    output_and_series_dict = output_and_series.dict()

    assert output_and_series_dict["node_name"] == "and_1"
    and_node = get_node(output_and_series_dict["graph"], "and_1")
    assert and_node == {
        "name": "and_1",
        "type": NodeType.AND,
        "parameters": {"value": None},
        "output_type": NodeOutputType.SERIES,
    }
    assert output_and_series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "and_1"},
        {"source": "project_1", "target": "and_1"},
    ]

    output_or_scalar = bool_series | False
    assert output_or_scalar.name is None
    assert output_or_scalar.parent is None
    output_or_scalar_dict = output_or_scalar.dict()
    assert output_or_scalar_dict["node_name"] == "or_1"
    node = get_node(output_or_scalar_dict["graph"], "or_1")
    assert node == {
        "name": "or_1",
        "type": NodeType.OR,
        "parameters": {"value": False},
        "output_type": NodeOutputType.SERIES,
    }
    assert output_or_scalar_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "or_1"},
    ]

    with pytest.raises(TypeError) as exc:
        _ = bool_series & "string"
    expected_msg = (
        'type of argument "other" must be one of (bool, featurebyte.core.series.FrozenSeries); '
        "got str instead"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = bool_series | int_series
    expected_msg = (
        f"Not supported operation 'or' between 'Series[BOOL](name=MASK, node_name={bool_series.node.name})' and "
        f"'Series[INT](name=CUST_ID, node_name={int_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


def _check_node_equality(
    left_node, right_node, exclude, has_value_params=False, has_right_op=False, expected_value=None
):
    """
    Check left node & right node equality
    """
    left_node_dict = left_node.dict(exclude=exclude)
    assert left_node_dict == right_node.dict(exclude=exclude)
    parameters = left_node_dict["parameters"]
    if has_value_params:
        assert "value" in parameters
        assert parameters["value"] == expected_value
    if has_right_op:
        assert "right_op" in parameters


def test_relational_operators__series_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as Series type
    """
    # pylint: disable=comparison-with-itself
    series_bool_eq = bool_series == bool_series
    series_int_ne = int_series != int_series
    series_float_lt = float_series < float_series
    series_float_lt_int = float_series < int_series
    series_varchar_le = varchar_series <= varchar_series
    series_bool_gt = bool_series > bool_series
    series_int_ge = int_series >= int_series
    series_int_ge_float = int_series >= float_series
    assert series_bool_eq.dtype == DBVarType.BOOL
    assert series_int_ne.dtype == DBVarType.BOOL
    assert series_float_lt.dtype == DBVarType.BOOL
    assert series_varchar_le.dtype == DBVarType.BOOL
    assert series_bool_gt.dtype == DBVarType.BOOL
    assert series_int_ge.dtype == DBVarType.BOOL
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        series_bool_eq.node,
        construct_node(name="eq_1", type=NodeType.EQ, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_ne.node,
        construct_node(name="ne_1", type=NodeType.NE, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_lt.node,
        construct_node(name="lt_1", type=NodeType.LT, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_lt_int.node,
        construct_node(name="lt_2", type=NodeType.LT, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_varchar_le.node,
        construct_node(name="le_1", type=NodeType.LE, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_bool_gt.node,
        construct_node(name="gt_1", type=NodeType.GT, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_ge.node,
        construct_node(name="ge_1", type=NodeType.GE, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_ge_float.node,
        construct_node(name="ge_2", type=NodeType.GE, **node_kwargs),
        exclude=exclude,
    )


def test_relational_operators__scalar_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as scalar type
    """
    scalar_float_eq = float_series == 1.234
    scalar_varchar_ne = varchar_series != "hello"
    scalar_bool_lt = bool_series < True
    scalar_int_le = int_series <= 100
    scalar_int_le_float = int_series <= 100.0
    scalar_float_gt = float_series > 1.234
    scalar_float_gt_int = float_series > 1
    scalar_varchar_ge = varchar_series >= "world"
    assert scalar_float_eq.dtype == DBVarType.BOOL
    assert scalar_varchar_ne.dtype == DBVarType.BOOL
    assert scalar_bool_lt.dtype == DBVarType.BOOL
    assert scalar_int_le.dtype == DBVarType.BOOL
    assert scalar_float_gt.dtype == DBVarType.BOOL
    assert scalar_varchar_ge.dtype == DBVarType.BOOL
    kwargs = {"output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        scalar_float_eq.node,
        construct_node(name="eq_1", type=NodeType.EQ, parameters={"value": 1.234}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=1.234,
    )
    _check_node_equality(
        scalar_varchar_ne.node,
        construct_node(name="ne_1", type=NodeType.NE, parameters={"value": "hello"}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value="hello",
    )
    _check_node_equality(
        scalar_bool_lt.node,
        construct_node(name="lt_1", type=NodeType.LT, parameters={"value": True}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=True,
    )
    _check_node_equality(
        scalar_int_le.node,
        construct_node(name="le_1", type=NodeType.LE, parameters={"value": 100}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=100,
    )
    _check_node_equality(
        scalar_int_le_float.node,
        construct_node(name="le_2", type=NodeType.LE, parameters={"value": 100.0}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=100.0,
    )
    _check_node_equality(
        scalar_float_gt.node,
        construct_node(name="gt_1", type=NodeType.GT, parameters={"value": 1.234}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=1.234,
    )
    _check_node_equality(
        scalar_float_gt_int.node,
        construct_node(name="gt_2", type=NodeType.GT, parameters={"value": 1}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value=1,
    )
    _check_node_equality(
        scalar_varchar_ge.node,
        construct_node(name="ge_1", type=NodeType.GE, parameters={"value": "world"}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        expected_value="world",
    )

    with pytest.raises(TypeError) as exc:
        _ = int_series > varchar_series
    expected_msg = (
        f"Not supported operation 'gt' between 'Series[INT](name=CUST_ID, node_name={int_series.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node_name={varchar_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


def test_arithmetic_operators(int_series, float_series, varchar_series):
    """
    Test arithmetic operators with other as series or scalar type
    """
    series_int_float_add = int_series + float_series
    series_int_int_sub = int_series - int_series
    series_float_int_mul = float_series * int_series
    series_float_float_div = float_series / float_series
    series_varchar_varchar_add = varchar_series + varchar_series
    series_int_int_mod = int_series % int_series
    assert series_int_float_add.dtype == DBVarType.FLOAT
    assert series_int_int_sub.dtype == DBVarType.INT
    assert series_float_int_mul.dtype == DBVarType.FLOAT
    assert series_float_float_div.dtype == DBVarType.FLOAT
    assert series_varchar_varchar_add.dtype == DBVarType.VARCHAR
    assert series_int_int_mod.dtype == DBVarType.INT
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        series_int_float_add.node,
        construct_node(name="add_1", type=NodeType.ADD, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_int_sub.node,
        construct_node(name="sub_1", type=NodeType.SUB, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_int_mul.node,
        construct_node(name="mul_1", type=NodeType.MUL, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_float_div.node,
        construct_node(name="div_1", type=NodeType.DIV, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_varchar_varchar_add.node,
        construct_node(name="concat_1", type=NodeType.CONCAT, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_int_mod.node,
        construct_node(name="mod_1", type=NodeType.MOD, **node_kwargs),
        exclude=exclude,
    )

    scalar_int_float_add = int_series + 1.23
    scalar_int_int_sub = int_series - 1
    scalar_float_int_mul = float_series * 2
    scalar_float_float_div = float_series / 2.34
    scalar_varchar_varchar_add = varchar_series + "hello"
    scalar_int_int_mod = int_series % 3
    assert scalar_int_float_add.dtype == DBVarType.FLOAT
    assert scalar_int_int_sub.dtype == DBVarType.INT
    assert scalar_float_int_mul.dtype == DBVarType.FLOAT
    assert scalar_float_float_div.dtype == DBVarType.FLOAT
    assert scalar_varchar_varchar_add.dtype == DBVarType.VARCHAR
    assert scalar_int_int_mod.dtype == DBVarType.INT
    kwargs = {"output_type": NodeOutputType.SERIES}
    _check_node_equality(
        scalar_int_float_add.node,
        construct_node(name="add_2", type=NodeType.ADD, parameters={"value": 1.23}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=1.23,
    )
    _check_node_equality(
        scalar_int_int_sub.node,
        construct_node(name="sub_2", type=NodeType.SUB, parameters={"value": 1}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=1,
    )
    _check_node_equality(
        scalar_float_int_mul.node,
        construct_node(name="mul_2", type=NodeType.MUL, parameters={"value": 2}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=2,
    )
    _check_node_equality(
        scalar_float_float_div.node,
        construct_node(name="div_2", type=NodeType.DIV, parameters={"value": 2.34}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=2.34,
    )
    _check_node_equality(
        scalar_varchar_varchar_add.node,
        construct_node(
            name="concat_2", type=NodeType.CONCAT, parameters={"value": "hello"}, **kwargs
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value="hello",
    )
    _check_node_equality(
        scalar_int_int_mod.node,
        construct_node(name="mod_2", type=NodeType.MOD, parameters={"value": 3}, **kwargs),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=3,
    )


def test_right_arithmetic_operators(int_series, float_series, varchar_series):
    """
    Test arithmetic operators with other as series or scalar type (operation from the right object)
    """
    scalar_int_float_add = 1.23 + int_series
    scalar_int_int_sub = 1 - int_series
    scalar_float_int_mul = 2 * float_series
    scalar_float_float_div = 2.34 / float_series
    scalar_varchar_varchar_add = "abc" + varchar_series
    scalar_int_int_mod = 1234 % int_series
    assert scalar_int_float_add.dtype == DBVarType.FLOAT
    assert scalar_int_int_sub.dtype == DBVarType.INT
    assert scalar_float_int_mul.dtype == DBVarType.FLOAT
    assert scalar_float_float_div.dtype == DBVarType.FLOAT
    assert scalar_varchar_varchar_add.dtype == DBVarType.VARCHAR
    kwargs = {"output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        scalar_int_float_add.node,
        construct_node(
            name="add_1", type=NodeType.ADD, parameters={"value": 1.23, "right_op": True}, **kwargs
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=1.23,
    )
    _check_node_equality(
        scalar_int_int_sub.node,
        construct_node(
            name="sub_1", type=NodeType.SUB, parameters={"value": 1, "right_op": True}, **kwargs
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=1,
    )
    _check_node_equality(
        scalar_float_int_mul.node,
        construct_node(
            name="mul_1", type=NodeType.MUL, parameters={"value": 2, "right_op": True}, **kwargs
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=2,
    )
    _check_node_equality(
        scalar_float_float_div.node,
        construct_node(
            name="div_1", type=NodeType.DIV, parameters={"value": 2.34, "right_op": True}, **kwargs
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=2.34,
    )
    _check_node_equality(
        scalar_varchar_varchar_add.node,
        construct_node(
            name="concat_2",
            type=NodeType.CONCAT,
            parameters={"value": "abc", "right_op": True},
            **kwargs,
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value="abc",
    )
    _check_node_equality(
        scalar_int_int_mod.node,
        construct_node(
            name="mod_2",
            type=NodeType.MOD,
            parameters={"value": 1234, "right_op": True},
            **kwargs,
        ),
        exclude=exclude,
        has_value_params=True,
        has_right_op=True,
        expected_value=1234,
    )


def test_arithmetic_operators__types_not_supported(varchar_series, int_series):
    """
    Test arithmetic operators on not supported types
    """
    with pytest.raises(TypeError) as exc:
        _ = varchar_series * 1
    expected_msg = (
        f"Series[VARCHAR](name=PRODUCT_ACTION, node_name={varchar_series.node.name}) "
        f"does not support operation 'mul'."
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = int_series * varchar_series
    expected_msg = (
        f"Not supported operation 'mul' between "
        f"'Series[INT](name=CUST_ID, node_name={int_series.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node_name={varchar_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


def test_date_difference_operator(timestamp_series, timestamp_series_2):
    """
    Test difference between two date Series
    """
    date_diff_series = timestamp_series_2 - timestamp_series
    assert date_diff_series.dtype == DBVarType.TIMEDELTA
    _check_node_equality(
        date_diff_series.node,
        construct_node(
            name="date_diff_1",
            type=NodeType.DATE_DIFF,
            parameters={},
            output_type=NodeOutputType.SERIES,
        ),
        exclude={"name": True},
    )


def test_date_difference_operator__invalid(timestamp_series, float_series):
    """
    Test difference between a date and a non-date Series (invalid)
    """
    with pytest.raises(TypeError) as exc:
        _ = timestamp_series - float_series
    assert "does not support operation 'sub'." in str(exc.value)


def test_date_add_operator__date_diff_timedelta(timestamp_series, timedelta_series):
    """
    Test incrementing a date Series with a timedelta Series
    """
    new_series = timestamp_series + timedelta_series
    assert new_series.dtype == DBVarType.TIMESTAMP
    _check_node_equality(
        new_series.node,
        construct_node(
            name="date_add_1",
            type=NodeType.DATE_ADD,
            parameters={},
            output_type=NodeOutputType.SERIES,
        ),
        exclude={"name": True},
    )
    series_dict = new_series.dict()
    assert series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_1", "target": "date_diff_1"},
        {"source": "project_2", "target": "date_diff_1"},
        {"source": "project_1", "target": "date_add_1"},
        {"source": "date_diff_1", "target": "date_add_1"},
    ]


def test_date_add_operator__constructed_timedelta(timestamp_series, timedelta_series_from_int):
    """
    Test incrementing a date Series with a timedelta Series constructed using to_timedelta()
    """
    new_series = timestamp_series + timedelta_series_from_int
    assert new_series.dtype == DBVarType.TIMESTAMP
    _check_node_equality(
        new_series.node,
        construct_node(
            name="date_add_1",
            type=NodeType.DATE_ADD,
            parameters={},
            output_type=NodeOutputType.SERIES,
        ),
        exclude={"name": True},
    )
    series_dict = new_series.dict()
    assert series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "timedelta_1"},
        {"source": "input_1", "target": "project_2"},
        {"source": "project_2", "target": "date_add_1"},
        {"source": "timedelta_1", "target": "date_add_1"},
    ]


@pytest.mark.parametrize("right_side_op", [False, True])
def test_date_add_operator__scalar_timedelta(timestamp_series, right_side_op):
    """
    Test incrementing a date Series with a scalar timedelta value
    """
    if right_side_op:
        new_series = pd.Timedelta("1d") + timestamp_series
    else:
        new_series = timestamp_series + pd.Timedelta("1d")
    assert new_series.dtype == DBVarType.TIMESTAMP
    _check_node_equality(
        new_series.node,
        construct_node(
            name="date_add_1",
            type=NodeType.DATE_ADD,
            parameters={"value": 86400},
            output_type=NodeOutputType.SERIES,
        ),
        exclude={"name": True},
    )
    series_dict = new_series.dict()
    assert series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "date_add_1"},
    ]


def test_binary_op_between_different_series_classes_not_allowed(float_series, float_feature):
    """
    Test binary operation between Series of incompatible types is not allowed
    """
    with pytest.raises(TypeError) as exc:
        _ = float_series + float_feature

    assert str(exc.value) == "Operation between Series and Feature is not supported"


def assert_series_attributes_equal(left, right):
    """
    Check that common series attributes unrelated to transforms are the same
    """
    assert left.tabular_source == right.tabular_source
    assert left.feature_store == right.feature_store
    assert left.row_index_lineage == right.row_index_lineage


def test_isnull(bool_series):
    """
    Test isnull operation
    """
    result = bool_series.isnull()
    assert result.dtype == DBVarType.BOOL
    assert_series_attributes_equal(result, bool_series)
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        result.node,
        construct_node(name="is_null_1", type=NodeType.IS_NULL, **node_kwargs),
        exclude=exclude,
    )


def test_notnull(bool_series, expression_sql_template):
    """
    Test notnull operation
    """
    result = bool_series.notnull()
    assert result.dtype == DBVarType.BOOL
    assert_series_attributes_equal(result, bool_series)
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        result.node,
        construct_node(name="not_1", type=NodeType.NOT, **node_kwargs),
        exclude=exclude,
    )
    expected_sql = expression_sql_template.format(
        expression=textwrap.dedent(
            """
            NOT (
              "MASK" IS NULL
            )
            """
        ).strip()
    )
    assert expected_sql == result.preview_sql()


def test_fillna(float_series):
    """
    Test fillna operation
    """
    float_series.fillna(0.0)
    float_series_dict = float_series.dict()
    assert float_series_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "is_null_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "is_null_1", "target": "conditional_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "conditional_1", "target": "assign_1"},
        {"source": "assign_1", "target": "project_2"},
    ]


def test_series_copy(float_series):
    """
    Test series copy always makes a deep copy
    """
    assert float_series.feature_store is not None
    assert float_series.parent is not None
    new_float_series = float_series.copy()
    assert new_float_series == float_series
    assert new_float_series.feature_store == float_series.feature_store
    assert id(new_float_series.feature_store) != id(float_series.feature_store)
    assert id(new_float_series.graph.nodes) == id(float_series.graph.nodes)
    assert id(new_float_series.parent) != id(float_series.parent)

    # check for the series without parent
    feat = float_series + 1
    assert feat.feature_store is not None
    assert feat.parent is None
    new_feat = feat.copy()
    assert new_feat == feat
    assert id(new_feat.graph.nodes) == id(feat.graph.nodes) == id(float_series.graph.nodes)


@pytest.mark.parametrize(
    "scalar_input, expected_literal", [(True, "'scalar_value'"), (False, '"PRODUCT_ACTION"')]
)
def test_varchar_series_concat(varchar_series, scalar_input, expected_literal):
    """
    Test varchar series concat
    """
    if scalar_input:
        output_series = varchar_series + "scalar_value"
    else:
        output_series = varchar_series + varchar_series
    output_sql = output_series.preview_sql()
    assert (
        output_sql
        == textwrap.dedent(
            f"""
        SELECT
          (
            CONCAT("PRODUCT_ACTION", {expected_literal})
          )
        FROM "db"."public"."transaction"
        LIMIT 10
        """
        ).strip()
    )


@pytest.mark.parametrize(
    "series_fixture_name", ["float_series", "int_series", "bool_series", "varchar_series"]
)
def test_astype__expected_parameters(series_fixture_name, request):
    """
    Test series astype method
    """
    series = request.getfixturevalue(series_fixture_name)

    def _check_converted_series(converted_series, expected_type_in_params):
        assert converted_series.node.type == NodeType.CAST
        assert converted_series.node.parameters == {
            "type": expected_type_in_params,
            "from_dtype": series.dtype,
        }
        input_node_names = converted_series.graph.backward_edges_map[converted_series.node.name]
        assert input_node_names == [series.node.name]

    _check_converted_series(series.astype(int), "int")
    _check_converted_series(series.astype("int"), "int")
    _check_converted_series(series.astype(float), "float")
    _check_converted_series(series.astype("float"), "float")
    _check_converted_series(series.astype(str), "str")
    _check_converted_series(series.astype("str"), "str")


def test_astype__invalid_type_str(float_series):
    """
    Test series astype with invalid type specification
    """
    with pytest.raises(TypeError) as exc:
        float_series.astype("number")
    assert str(exc.value) == (
        'type of argument "new_type" must be one of (Type[int], Type[float], Type[str],'
        " Literal[int, float, str]); got str instead"
    )


def test_astype__invalid_type_cls(float_series):
    """
    Test series astype with invalid type specification
    """
    with pytest.raises(TypeError) as exc:
        float_series.astype(dict)
    assert str(exc.value) == (
        'type of argument "new_type" must be one of (Type[int], Type[float], Type[str],'
        " Literal[int, float, str]); got dict instead"
    )


@pytest.mark.parametrize(
    "series_fixture_name", ["timestamp_series", "timedelta_series", "timedelta_series"]
)
def test_astype__unsupported_dtype(series_fixture_name, request):
    series = request.getfixturevalue(series_fixture_name)
    with pytest.raises(TypeError) as exc:
        series.astype(int)
    assert str(exc.value) == f"astype not supported for {series.dtype}"


def test_node_types_lineage(dataframe, float_series):
    """
    Test node_types_lineage attribute
    """
    # check lineage of simple series
    assert float_series.node_types_lineage == ["project", "input"]

    # check lineage of series after unrelated operations in frame
    dataframe["new_series_add"] = float_series + 123
    dataframe["new_series_sub"] = float_series - 123
    new_series = dataframe["new_series_sub"]
    # only one "assign" and no "add" in the lineage
    assert new_series.node_types_lineage == ["project", "assign", "input", "sub", "project"]
    assert new_series.astype(str).node_types_lineage == [
        "cast",
        "project",
        "assign",
        "input",
        "sub",
        "project",
    ]


@pytest.mark.parametrize(
    "op_func, expected_node_type, expected_dtype, expected_params, expected_expr",
    [
        (lambda s: s.sqrt(), NodeType.SQRT, DBVarType.FLOAT, {}, 'SQRT("VALUE")'),
        (lambda s: s.abs(), NodeType.ABS, DBVarType.FLOAT, {}, 'ABS("VALUE")'),
        (
            lambda s: s.pow(2),
            NodeType.POWER,
            DBVarType.FLOAT,
            {"value": 2},
            """
            (
              POWER("VALUE", 2)
            )
            """,
        ),
        (
            lambda s: s**2,
            NodeType.POWER,
            DBVarType.FLOAT,
            {"value": 2},
            """
            (
              POWER("VALUE", 2)
            )
            """,
        ),
        (lambda s: s.log(), NodeType.LOG, DBVarType.FLOAT, {}, 'LN("VALUE")'),
        (lambda s: s.exp(), NodeType.EXP, DBVarType.FLOAT, {}, 'EXP("VALUE")'),
        (lambda s: s.floor(), NodeType.FLOOR, DBVarType.INT, {}, 'FLOOR("VALUE")'),
        (lambda s: s.ceil(), NodeType.CEIL, DBVarType.INT, {}, 'CEIL("VALUE")'),
    ],
)
def test_numeric_operations(
    float_series,
    expression_sql_template,
    op_func,
    expected_node_type,
    expected_dtype,
    expected_params,
    expected_expr,
):
    """Test numeric operations"""
    new_series = op_func(float_series)
    assert_series_attributes_equal(new_series, float_series)
    assert new_series.dtype == expected_dtype
    assert new_series.node.parameters.dict() == expected_params
    assert new_series.node.type == expected_node_type
    assert new_series.node.output_type == NodeOutputType.SERIES
    expected_sql = expression_sql_template.format(expression=expected_expr)
    assert expected_sql == new_series.preview_sql()


@pytest.mark.parametrize("method", ["sqrt", "abs", "floor", "ceil"])
def test_numeric__invalid_dtype(bool_series, method):
    """Test numeric operations cannot be applied to non-numeric series"""
    with pytest.raises(TypeError) as exc:
        _ = getattr(bool_series, method)()
    assert str(exc.value) == f"{method} is only available to numeric Series; got BOOL"


@pytest.fixture(name="scalar_timestamp")
def scalar_timestamp_fixture():
    """
    Fixture for a scalar timestamp value
    """
    return pd.Timestamp("2023-01-15 10:00:00")


@pytest.fixture(name="scalar_timestamp_tz")
def scalar_timestamp_tz_fixture():
    """
    Fixture for a scalar timestamp value with timezone
    """
    return pd.Timestamp("2023-01-15 10:00:00", tz="Asia/Singapore")


@pytest.mark.parametrize(
    "series_fixture_name", ["float_series", "int_series", "bool_series", "varchar_series"]
)
def test_scalar_timestamp__invalid_with_wrong_type(request, series_fixture_name, scalar_timestamp):
    """
    Test scalar timestamp value cannot be compared with series of wrong type
    """
    series = request.getfixturevalue(series_fixture_name)
    with pytest.raises(TypeError) as exc:
        _ = series > scalar_timestamp
    assert "Not supported operation" in str(exc.value)


@pytest.mark.parametrize(
    "op_func, expected_operator_str",
    [
        (lambda s, ts_value: s > ts_value, ">"),
        (lambda s, ts_value: s >= ts_value, ">="),
        (lambda s, ts_value: s < ts_value, "<"),
        (lambda s, ts_value: s <= ts_value, "<="),
        (lambda s, ts_value: s == ts_value, "="),
        (lambda s, ts_value: s != ts_value, "<>"),
    ],
)
def test_scalar_timestamp__valid(
    timestamp_series, scalar_timestamp, op_func, expected_operator_str
):
    """
    Test scalar timestamp value in a relational operation
    """
    result = op_func(timestamp_series, scalar_timestamp)
    assert result.node.parameters.value.dict() == {
        "iso_format_str": "2023-01-15T10:00:00",
        "type": "timestamp",
    }
    expected = textwrap.dedent(
        f"""
        SELECT
          (
            "TIMESTAMP" {expected_operator_str} TO_TIMESTAMP('2023-01-15T10:00:00')
          )
        FROM "db"."public"."transaction"
        LIMIT 10
        """
    ).strip()
    assert result.preview_sql() == expected


def test_scalar_timestamp__with_tz(timestamp_series, scalar_timestamp_tz):
    """
    Test scalar timestamp value with timezone in a relational operation
    """
    result = timestamp_series > scalar_timestamp_tz
    assert result.node.parameters.value.dict() == {
        "iso_format_str": "2023-01-15T10:00:00+08:00",
        "type": "timestamp",
    }
    expected = textwrap.dedent(
        """
        SELECT
          (
            "TIMESTAMP" > TO_TIMESTAMP('2023-01-15T02:00:00')
          )
        FROM "db"."public"."transaction"
        LIMIT 10
        """
    ).strip()
    assert result.preview_sql() == expected


def test_scalar_timestamp__invalid(timestamp_series, scalar_timestamp):
    """
    Test scalar timestamp value is not allowed in a non-relational operation
    """
    with pytest.raises(TypeError) as exc:
        _ = timestamp_series - scalar_timestamp
    assert (
        'type of argument "other" must be one of (int, float, featurebyte.core.series.FrozenSeries)'
        in str(exc.value)
    )


def test_operation_structure_cache(float_series):
    """
    Test operation_structure property is cached
    """
    new_series = (float_series + 123.45) * 678.90

    with patch("featurebyte.query_graph.graph.OperationStructureExtractor") as mock_cls:
        mock_cls.side_effect = OperationStructureExtractor
        op_struct_1 = new_series.operation_structure
        op_struct_2 = new_series.operation_structure
        _ = new_series.row_index_lineage
        _ = new_series.output_category

    assert op_struct_1 == op_struct_2
    assert mock_cls.call_count == 1
