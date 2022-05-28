"""
Unit test for Series
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


def test__getitem__series_key(int_series, bool_series):
    """
    Test filtering using boolean Series
    """
    series = int_series[bool_series]
    assert series.node == Node(name="filter_1", type="filter", parameters={}, output_type="series")
    assert series.name == int_series.name
    assert series.var_type == int_series.var_type
    assert dict(series.graph.edges) == {
        "input_1": ["project_1", "project_2"],
        "project_1": ["filter_1"],
        "project_2": ["filter_1"],
    }


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
    assert filtered_bool_series.var_type == DBVarType.BOOL
    with pytest.raises(ValueError) as exc:
        _ = int_series[filtered_bool_series]
    assert "Row index not aligned!" in str(exc.value)


def test__getitem__type_not_supported(int_series):
    """
    Test retrieval with unsupported type
    """
    with pytest.raises(TypeError) as exc:
        _ = int_series[True]
    assert "Type <class 'bool'> not supported!" in str(exc.value)


@pytest.mark.parametrize(
    "column,value",
    [
        ("CUST_ID", 100),
        ("PRODUCT_ACTION", "string_val"),
        ("VALUE", 10),
        ("VALUE", 1.23),
    ],
)
def test__setitem__bool_series_key_scalar_value(dataframe, bool_series, column, value):
    """
    Test Series conditional assignment
    """
    series = dataframe[column]
    series[bool_series] = value
    assert series.node == Node(
        name="cond_assign_1",
        type=NodeType.COND_ASSIGN,
        parameters={"value": value},
        output_type=NodeOutputType.SERIES,
    )
    assert dict(series.graph.edges) == {
        "input_1": ["project_1", "project_2"],
        "project_1": ["cond_assign_1"],
        "project_2": ["cond_assign_1"],
    }


def test__setitem__cond_assign_with_same_input_nodes(bool_series):
    """
    Test Series conditional assignment using same series for filtering & assignment
    """
    bool_series[bool_series] = True
    assert dict(bool_series.graph.edges) == {
        "input_1": ["project_1"],
        "project_1": ["cond_assign_1", "cond_assign_1"],
    }
    assert dict(bool_series.graph.backward_edges) == {
        "project_1": ["input_1"],
        "cond_assign_1": ["project_1", "project_1"],
    }


def test__setitem__row_index_not_aligned(int_series, bool_series):
    """
    Test conditional assignment using non-aligned series
    """
    filtered_bool_series = bool_series[bool_series]
    with pytest.raises(ValueError) as exc:
        int_series[filtered_bool_series] = 1
    assert "Row index not aligned!" in str(exc.value)


def test__setitem__non_boolean_series_type_not_supported(int_series, bool_series):
    """
    Test conditional assignment using non-boolean series
    """
    with pytest.raises(TypeError) as exc:
        bool_series[int_series] = True
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test_logical_operators(bool_series, int_series):
    """
    Test logical operators
    """
    output_and_series = bool_series & bool_series
    assert output_and_series.name is None
    assert output_and_series.node == Node(
        name="and_1", type=NodeType.AND, parameters={}, output_type=NodeOutputType.SERIES
    )
    assert dict(output_and_series.graph.edges) == {
        "input_1": ["project_1", "project_2"],
        "project_1": ["and_1", "and_1"],
    }
    assert dict(output_and_series.graph.backward_edges) == {
        "project_1": ["input_1"],
        "project_2": ["input_1"],
        "and_1": ["project_1", "project_1"],
    }

    output_or_scalar = bool_series | False
    assert output_or_scalar.name is None
    assert output_or_scalar.node == Node(
        name="or_1",
        type=NodeType.OR,
        parameters={"value": False},
        output_type=NodeOutputType.SERIES,
    )
    assert dict(output_or_scalar.graph.edges) == {
        "input_1": ["project_1", "project_2"],
        "project_1": ["and_1", "and_1", "or_1"],
    }
    assert dict(output_or_scalar.graph.backward_edges) == {
        "project_1": ["input_1"],
        "project_2": ["input_1"],
        "and_1": ["project_1", "project_1"],
        "or_1": ["project_1"],
    }

    with pytest.raises(TypeError) as exc:
        _ = bool_series & "string"
    assert "Not supported operation 'and' between BOOL and <class 'str'>!" in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = bool_series | int_series
    expected_msg = "Not supported operation 'or' between BOOL and <class 'featurebyte.pandas.series.Series'>[INT]!"
    assert expected_msg in str(exc.value)


def test_relational_operators__series_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as Series type
    """
    # pylint: disable=R0124
    # series_bool_eq = bool_series == bool_series
    # series_int_ne = int_series != int_series
    series_float_lt = float_series < float_series
    series_varchar_le = varchar_series <= varchar_series
    series_bool_gt = bool_series > bool_series
    series_int_ge = int_series >= int_series
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    # assert series_bool_eq.node == Node(name="eq_1", type=NodeType.EQ, **node_kwargs)
    # assert series_int_ne.node == Node(name="ne_1", type=NodeType.NE, **node_kwargs)
    assert series_float_lt.node == Node(name="lt_1", type=NodeType.LT, **node_kwargs)
    assert series_varchar_le.node == Node(name="le_1", type=NodeType.LE, **node_kwargs)
    assert series_bool_gt.node == Node(name="gt_1", type=NodeType.GT, **node_kwargs)
    assert series_int_ge.node == Node(name="ge_1", type=NodeType.GE, **node_kwargs)
    # assert series_bool_eq.var_type == DBVarType.BOOL
    # assert series_int_ne.var_type == DBVarType.INT
    assert series_float_lt.var_type == DBVarType.BOOL
    assert series_varchar_le.var_type == DBVarType.BOOL
    assert series_bool_gt.var_type == DBVarType.BOOL
    assert series_int_ge.var_type == DBVarType.BOOL


def test_relational_operators__scalar_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as scalar type
    """
    # scalar_float_eq = float_series == 1.234
    # scalar_varchar_ne = varchar_series != "hello"
    scalar_bool_lt = bool_series < True
    scalar_int_le = int_series <= 100
    scalar_float_gt = float_series > 1.234
    scalar_varchar_ge = varchar_series >= "world"
    kwargs = {"output_type": NodeOutputType.SERIES}
    # assert scalar_float_eq.node == Node(
    #     name="eq_1", type=NodeType.EQ, parameters={"value": 1.234}, **kwargs
    # )
    # assert scalar_varchar_ne.node == Node(
    #     name="ne_1", type=NodeType.NE, parameters={"value": "hello"}, **kwargs
    # )
    assert scalar_bool_lt.node == Node(
        name="lt_1", type=NodeType.LT, parameters={"value": True}, **kwargs
    )
    assert scalar_int_le.node == Node(
        name="le_1", type=NodeType.LE, parameters={"value": 100}, **kwargs
    )
    assert scalar_float_gt.node == Node(
        name="gt_1", type=NodeType.GT, parameters={"value": 1.234}, **kwargs
    )
    assert scalar_varchar_ge.node == Node(
        name="ge_1", type=NodeType.GE, parameters={"value": "world"}, **kwargs
    )
    # assert scalar_float_eq.var_type == DBVarType.BOOL
    # assert scalar_varchar_ne.var_type == DBVarType.BOOL
    assert scalar_bool_lt.var_type == DBVarType.BOOL
    assert scalar_int_le.var_type == DBVarType.BOOL
    assert scalar_float_gt.var_type == DBVarType.BOOL
    assert scalar_varchar_ge.var_type == DBVarType.BOOL

    with pytest.raises(TypeError) as exc:
        _ = int_series > varchar_series
    expected_msg = "Not supported operation 'gt' between INT and <class 'featurebyte.pandas.series.Series'>[VARCHAR]!"
    assert expected_msg in str(exc.value)


def test_arithmetic_operators(int_series, float_series):
    """
    Test arithmetic operators with other as series or scalar type
    """
    series_int_float_add = int_series + float_series
    series_int_int_sub = int_series - int_series
    series_float_int_mul = float_series * int_series
    series_float_float_div = float_series / float_series
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    assert series_int_float_add.var_type == DBVarType.FLOAT
    assert series_int_int_sub.var_type == DBVarType.INT
    assert series_float_int_mul.var_type == DBVarType.FLOAT
    assert series_float_float_div.var_type == DBVarType.FLOAT
    assert series_int_float_add.node == Node(name="add_1", type=NodeType.ADD, **node_kwargs)
    assert series_int_int_sub.node == Node(name="sub_1", type=NodeType.SUB, **node_kwargs)
    assert series_float_int_mul.node == Node(name="mul_1", type=NodeType.MUL, **node_kwargs)
    assert series_float_float_div.node == Node(name="div_1", type=NodeType.DIV, **node_kwargs)

    scalar_int_float_add = int_series + 1.23
    scalar_int_int_sub = int_series - 1
    scalar_float_int_mul = float_series * 2
    scalar_float_float_div = float_series / 2.34
    kwargs = {"output_type": NodeOutputType.SERIES}
    assert scalar_int_float_add.var_type == DBVarType.FLOAT
    assert scalar_int_int_sub.var_type == DBVarType.INT
    assert scalar_float_int_mul.var_type == DBVarType.FLOAT
    assert scalar_float_float_div.var_type == DBVarType.FLOAT
    assert scalar_int_float_add.node == Node(
        name="add_2", type=NodeType.ADD, parameters={"value": 1.23}, **kwargs
    )
    assert scalar_int_int_sub.node == Node(
        name="sub_2", type=NodeType.SUB, parameters={"value": 1}, **kwargs
    )
    assert scalar_float_int_mul.node == Node(
        name="mul_2", type=NodeType.MUL, parameters={"value": 2}, **kwargs
    )
    assert scalar_float_float_div.node == Node(
        name="div_2", type=NodeType.DIV, parameters={"value": 2.34}, **kwargs
    )


def test_arithmetic_operators__types_not_supported(varchar_series, int_series):
    """
    Test arithmetic operators on not supported types
    """
    with pytest.raises(TypeError) as exc:
        _ = varchar_series * 1
    assert "VARCHAR does not support operation 'mul'." in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = int_series * varchar_series
    expected_msg = "Not supported operation 'mul' between INT and <class 'featurebyte.pandas.series.Series'>[VARCHAR]!"
    assert expected_msg in str(exc.value)
