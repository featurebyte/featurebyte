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
    assert series.parent is None
    series_dict = series.dict()
    assert (
        series_dict["node"].items()
        >= {"type": "filter", "parameters": {}, "output_type": "series"}.items()
    )
    assert series_dict["name"] == int_series.name
    assert series_dict["var_type"] == int_series.var_type
    assert dict(series_dict["graph"]["edges"]) == {
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
    assert filtered_bool_series.parent is None
    with pytest.raises(ValueError) as exc:
        _ = int_series[filtered_bool_series]
    expected_msg = (
        f"Row indices between 'Series[INT](name=CUST_ID, node.name={int_series.node.name})' and "
        f"'Series[BOOL](name=MASK, node.name={filtered_bool_series.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__getitem__type_not_supported(int_series):
    """
    Test retrieval with unsupported type
    """
    with pytest.raises(KeyError) as exc:
        _ = int_series[True]
    assert "Series indexing with value 'True' not supported!" in str(exc.value)


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
    assert series.parent is dataframe
    series[bool_series] = value
    assert series.parent is dataframe
    series_dict = series.dict()
    assert series_dict["node"] == {
        "name": "project_3",
        "type": "project",
        "parameters": {"columns": [column]},
        "output_type": "series",
    }
    assert series_dict["graph"]["nodes"]["conditional_1"] == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": value},
        "output_type": "series",
    }
    assert dict(series.graph.edges) == {
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["conditional_1"],
        "project_2": ["conditional_1"],
        "conditional_1": ["assign_1"],
        "assign_1": ["project_3"],
    }


def test__setitem__cond_assign_with_same_input_nodes(bool_series):
    """
    Test Series conditional assignment using same series for filtering & assignment
    """
    bool_series[bool_series] = True
    assert bool_series.parent is not None
    bool_series_dict = bool_series.dict()
    assert dict(bool_series_dict["graph"]["edges"]) == {
        "assign_1": ["project_2"],
        "conditional_1": ["assign_1"],
        "input_1": ["project_1", "assign_1"],
        "project_1": ["conditional_1", "conditional_1"],
    }
    assert dict(bool_series_dict["graph"]["backward_edges"]) == {
        "assign_1": ["input_1", "conditional_1"],
        "conditional_1": ["project_1", "project_1"],
        "project_1": ["input_1"],
        "project_2": ["assign_1"],
    }


def test__setitem__cond_assign_consecutive(dataframe, bool_series):
    """
    Test Series conditional assignment consecutive operations
    """
    series = dataframe["VALUE"]
    series[bool_series] = 100
    series[bool_series] = 200
    series_dict = series.dict()
    assert series_dict["graph"]["nodes"]["conditional_1"] == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 100},
        "output_type": "series",
    }
    assert series_dict["graph"]["nodes"]["conditional_2"] == {
        "name": "conditional_2",
        "type": "conditional",
        "parameters": {"value": 200},
        "output_type": "series",
    }
    assert dict(series_dict["graph"]["backward_edges"]) == {
        "assign_1": ["input_1", "conditional_1"],
        "assign_2": ["assign_1", "conditional_2"],
        "conditional_1": ["project_1", "project_2"],
        "conditional_2": ["project_3", "project_2"],
        "project_1": ["input_1"],
        "project_2": ["input_1"],
        "project_3": ["assign_1"],
        "project_4": ["assign_2"],
    }


def test__setitem__conditional_assign_unnamed_series(int_series, bool_series):
    """
    Test conditional assign on a temporary series
    """
    temp_series = int_series + 1234
    temp_series[bool_series] = 0
    temp_series_dict = temp_series.dict()
    # Unnamed series stays unnamed (not a PROJECT node)
    assert temp_series_dict["node"] == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0},
        "type": "conditional",
    }
    # No assignment occurred
    assert temp_series_dict["graph"]["backward_edges"] == {
        "add_1": ["project_1"],
        "conditional_1": ["add_1", "project_2"],
        "project_1": ["input_1"],
        "project_2": ["input_1"],
    }


def test__setitem__row_index_not_aligned(int_series, bool_series):
    """
    Test conditional assignment using non-aligned series
    """
    filtered_bool_series = bool_series[bool_series]
    assert filtered_bool_series.parent is None
    with pytest.raises(ValueError) as exc:
        int_series[filtered_bool_series] = 1
    expected_msg = (
        f"Row indices between 'Series[INT](name=CUST_ID, node.name={int_series.node.name})' and "
        f"'Series[BOOL](name=MASK, node.name={filtered_bool_series.node.name})' are not aligned!"
    )
    assert expected_msg in str(exc.value)


def test__setitem__value_type_not_correct(int_series, bool_series):
    """
    Test conditional assignment when the assigned value not valid for given Series type
    """
    with pytest.raises(ValueError) as exc:
        int_series[bool_series] = "abc"
    expected_msg = f"Setting key 'Series[BOOL](name=MASK, node.name={bool_series.node.name})' with value 'abc' not supported!"
    assert expected_msg in str(exc.value)


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
    assert "Setting key '1' with value 'True' not supported!" in str(exc.value)


def test_logical_operators(bool_series, int_series):
    """
    Test logical operators
    """
    output_and_series = bool_series & bool_series
    assert output_and_series.name is None
    assert output_and_series.parent is None
    output_and_series_dict = output_and_series.dict()
    assert (
        output_and_series_dict["node"].items()
        >= {"type": NodeType.AND, "parameters": {}, "output_type": NodeOutputType.SERIES}.items()
    )
    assert dict(output_and_series_dict["graph"]["edges"]) == {
        "input_1": ["project_1"],
        "project_1": ["and_1", "and_1"],
    }
    assert dict(output_and_series_dict["graph"]["backward_edges"]) == {
        "project_1": ["input_1"],
        "and_1": ["project_1", "project_1"],
    }

    output_or_scalar = bool_series | False
    assert output_or_scalar.name is None
    assert output_or_scalar.parent is None
    output_or_scalar_dict = output_or_scalar.dict()
    assert (
        output_or_scalar_dict["node"].items()
        >= {
            "type": NodeType.OR,
            "parameters": {"value": False},
            "output_type": NodeOutputType.SERIES,
        }.items()
    )
    assert dict(output_or_scalar_dict["graph"]["edges"]) == {
        "input_1": ["project_1"],
        "project_1": ["or_1"],
    }
    assert dict(output_or_scalar_dict["graph"]["backward_edges"]) == {
        "project_1": ["input_1"],
        "or_1": ["project_1"],
    }

    with pytest.raises(TypeError) as exc:
        _ = bool_series & "string"
    expected_msg = (
        f"Not supported operation 'and' between "
        f"'Series[BOOL](name=MASK, node.name={bool_series.node.name})' and 'string'!"
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = bool_series | int_series
    expected_msg = (
        f"Not supported operation 'or' between 'Series[BOOL](name=MASK, node.name={bool_series.node.name})' and "
        f"'Series[INT](name=CUST_ID, node.name={int_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


def _check_node_equality(left_node, right_node, exclude):
    """
    Check left node & right node equality
    """
    assert left_node.dict(exclude=exclude) == right_node.dict(exclude=exclude)


def test_relational_operators__series_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as Series type
    """
    # pylint: disable=comparison-with-itself
    series_bool_eq = bool_series == bool_series
    series_int_ne = int_series != int_series
    series_float_lt = float_series < float_series
    series_varchar_le = varchar_series <= varchar_series
    series_bool_gt = bool_series > bool_series
    series_int_ge = int_series >= int_series
    assert series_bool_eq.var_type == DBVarType.BOOL
    assert series_int_ne.var_type == DBVarType.BOOL
    assert series_float_lt.var_type == DBVarType.BOOL
    assert series_varchar_le.var_type == DBVarType.BOOL
    assert series_bool_gt.var_type == DBVarType.BOOL
    assert series_int_ge.var_type == DBVarType.BOOL
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        series_bool_eq.node, Node(name="eq_1", type=NodeType.EQ, **node_kwargs), exclude=exclude
    )
    _check_node_equality(
        series_int_ne.node, Node(name="ne_1", type=NodeType.NE, **node_kwargs), exclude=exclude
    )
    _check_node_equality(
        series_float_lt.node, Node(name="lt_1", type=NodeType.LT, **node_kwargs), exclude=exclude
    )
    _check_node_equality(
        series_varchar_le.node, Node(name="le_1", type=NodeType.LE, **node_kwargs), exclude=exclude
    )
    _check_node_equality(
        series_bool_gt.node, Node(name="gt_1", type=NodeType.GT, **node_kwargs), exclude=exclude
    )
    _check_node_equality(
        series_int_ge.node, Node(name="ge_1", type=NodeType.GE, **node_kwargs), exclude=exclude
    )


def test_relational_operators__scalar_other(bool_series, int_series, float_series, varchar_series):
    """
    Test relational operators with other as scalar type
    """
    scalar_float_eq = float_series == 1.234
    scalar_varchar_ne = varchar_series != "hello"
    scalar_bool_lt = bool_series < True
    scalar_int_le = int_series <= 100
    scalar_float_gt = float_series > 1.234
    scalar_varchar_ge = varchar_series >= "world"
    assert scalar_float_eq.var_type == DBVarType.BOOL
    assert scalar_varchar_ne.var_type == DBVarType.BOOL
    assert scalar_bool_lt.var_type == DBVarType.BOOL
    assert scalar_int_le.var_type == DBVarType.BOOL
    assert scalar_float_gt.var_type == DBVarType.BOOL
    assert scalar_varchar_ge.var_type == DBVarType.BOOL
    kwargs = {"output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        scalar_float_eq.node,
        Node(name="eq_1", type=NodeType.EQ, parameters={"value": 1.234}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_varchar_ne.node,
        Node(name="ne_1", type=NodeType.NE, parameters={"value": "hello"}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_bool_lt.node,
        Node(name="lt_1", type=NodeType.LT, parameters={"value": True}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_int_le.node,
        Node(name="le_1", type=NodeType.LE, parameters={"value": 100}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_float_gt.node,
        Node(name="gt_1", type=NodeType.GT, parameters={"value": 1.234}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_varchar_ge.node,
        Node(name="ge_1", type=NodeType.GE, parameters={"value": "world"}, **kwargs),
        exclude=exclude,
    )

    with pytest.raises(TypeError) as exc:
        _ = int_series > varchar_series
    expected_msg = (
        f"Not supported operation 'gt' between 'Series[INT](name=CUST_ID, node.name={int_series.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node.name={varchar_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


def test_arithmetic_operators(int_series, float_series):
    """
    Test arithmetic operators with other as series or scalar type
    """
    series_int_float_add = int_series + float_series
    series_int_int_sub = int_series - int_series
    series_float_int_mul = float_series * int_series
    series_float_float_div = float_series / float_series
    assert series_int_float_add.var_type == DBVarType.FLOAT
    assert series_int_int_sub.var_type == DBVarType.INT
    assert series_float_int_mul.var_type == DBVarType.FLOAT
    assert series_float_float_div.var_type == DBVarType.FLOAT
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        series_int_float_add.node,
        Node(name="add_1", type=NodeType.ADD, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_int_int_sub.node,
        Node(name="sub_1", type=NodeType.SUB, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_int_mul.node,
        Node(name="mul_1", type=NodeType.MUL, **node_kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        series_float_float_div.node,
        Node(name="div_1", type=NodeType.DIV, **node_kwargs),
        exclude=exclude,
    )

    scalar_int_float_add = int_series + 1.23
    scalar_int_int_sub = int_series - 1
    scalar_float_int_mul = float_series * 2
    scalar_float_float_div = float_series / 2.34
    assert scalar_int_float_add.var_type == DBVarType.FLOAT
    assert scalar_int_int_sub.var_type == DBVarType.INT
    assert scalar_float_int_mul.var_type == DBVarType.FLOAT
    assert scalar_float_float_div.var_type == DBVarType.FLOAT
    kwargs = {"output_type": NodeOutputType.SERIES}
    _check_node_equality(
        scalar_int_float_add.node,
        Node(name="add_2", type=NodeType.ADD, parameters={"value": 1.23}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_int_int_sub.node,
        Node(name="sub_2", type=NodeType.SUB, parameters={"value": 1}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_float_int_mul.node,
        Node(name="mul_2", type=NodeType.MUL, parameters={"value": 2}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_float_float_div.node,
        Node(name="div_2", type=NodeType.DIV, parameters={"value": 2.34}, **kwargs),
        exclude=exclude,
    )


def test_right_arithmetic_operators(int_series, float_series):
    """
    Test arithmetic operators with other as series or scalar type (operation from the right object)
    """
    scalar_int_float_add = 1.23 + int_series
    scalar_int_int_sub = 1 - int_series
    scalar_float_int_mul = 2 * float_series
    scalar_float_float_div = 2.34 / float_series
    assert scalar_int_float_add.var_type == DBVarType.FLOAT
    assert scalar_int_int_sub.var_type == DBVarType.INT
    assert scalar_float_int_mul.var_type == DBVarType.FLOAT
    assert scalar_float_float_div.var_type == DBVarType.FLOAT
    kwargs = {"output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        scalar_int_float_add.node,
        Node(
            name="add_1", type=NodeType.ADD, parameters={"value": 1.23, "right_op": True}, **kwargs
        ),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_int_int_sub.node,
        Node(name="sub_1", type=NodeType.SUB, parameters={"value": 1, "right_op": True}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_float_int_mul.node,
        Node(name="mul_1", type=NodeType.MUL, parameters={"value": 2, "right_op": True}, **kwargs),
        exclude=exclude,
    )
    _check_node_equality(
        scalar_float_float_div.node,
        Node(
            name="div_1", type=NodeType.DIV, parameters={"value": 2.34, "right_op": True}, **kwargs
        ),
        exclude=exclude,
    )


def test_arithmetic_operators__types_not_supported(varchar_series, int_series):
    """
    Test arithmetic operators on not supported types
    """
    with pytest.raises(TypeError) as exc:
        _ = varchar_series * 1
    expected_msg = (
        f"Series[VARCHAR](name=PRODUCT_ACTION, node.name={varchar_series.node.name}) "
        f"does not support operation 'mul'."
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(TypeError) as exc:
        _ = int_series * varchar_series
    expected_msg = (
        f"Not supported operation 'mul' between "
        f"'Series[INT](name=CUST_ID, node.name={int_series.node.name})' and "
        f"'Series[VARCHAR](name=PRODUCT_ACTION, node.name={varchar_series.node.name})'!"
    )
    assert expected_msg in str(exc.value)


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
    assert result.var_type == DBVarType.BOOL
    assert_series_attributes_equal(result, bool_series)
    node_kwargs = {"parameters": {}, "output_type": NodeOutputType.SERIES}
    exclude = {"name": True}
    _check_node_equality(
        result.node,
        Node(name="is_null_1", type=NodeType.IS_NULL, **node_kwargs),
        exclude=exclude,
    )


def test_fillna(float_series):
    """
    Test fillna operation
    """
    float_series.fillna(0.0)
    float_series_dict = float_series.dict()
    assert float_series_dict["graph"]["backward_edges"] == {
        "project_1": ["input_1"],
        "is_null_1": ["project_1"],
        "conditional_1": ["project_1", "is_null_1"],
        "assign_1": ["input_1", "conditional_1"],
        "project_2": ["assign_1"],
    }


def test_series_copy(float_series):
    """
    Test series copy
    """
    assert float_series.feature_store is not None
    assert float_series.parent is not None
    new_float_series = float_series.copy()
    assert new_float_series == float_series
    assert new_float_series.feature_store == float_series.feature_store
    assert id(new_float_series.feature_store) != id(float_series.feature_store)
    assert id(new_float_series.graph.nodes) == id(float_series.graph.nodes)

    # check for the series without parent
    feat = float_series + 1
    assert feat.feature_store is not None
    assert feat.parent is None
    new_feat = feat.copy()
    assert new_feat == feat
    assert id(new_feat.graph.nodes) == id(feat.graph.nodes) == id(float_series.graph.nodes)

    # check that deepcopy is working
    deep_float_series = float_series.copy(deep=True)
    assert deep_float_series == float_series
    assert id(deep_float_series.graph.nodes) == id(float_series.graph.nodes)
