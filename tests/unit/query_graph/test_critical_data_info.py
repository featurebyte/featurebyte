"""
Tests for featurebyte.query_graph.graph_node.critical_data_info
"""
import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)


@pytest.mark.parametrize(
    "imputations",
    [
        # no imputation
        [],
        # single imputation
        [{"type": "missing", "imputed_value": 0}],
        [{"type": "disguised", "disguised_values": [-999, 999], "imputed_value": 0}],
        [{"type": "not_in", "expected_values": [1, 2, 3, 4], "imputed_value": 0}],
        [{"type": "less_than", "end_point": 0, "imputed_value": 0}],
        [{"type": "is_string", "imputed_value": None}],
        # multiple imputations
        [
            {"type": "missing", "imputed_value": 0},
            {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
        ],
        [
            {"type": "missing", "imputed_value": 0},
            {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
            {"type": "less_than", "end_point": 0, "imputed_value": 0},
        ],
    ],
)
def test_critical_data_info__valid_imputations(imputations):
    """Test multiple imputations (valid)"""
    cdi = CriticalDataInfo(cleaning_operations=imputations)
    assert cdi.dict() == {"cleaning_operations": imputations}


@pytest.mark.parametrize(
    "imputations,conflicts",
    [
        (
            [
                # invalid as -999 imputed to None first, then None is imputed to 0 (double imputation)
                {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
                {"type": "missing", "imputed_value": 0},
            ],
            ["DisguisedValueImputation", "MissingValueImputation"],
        ),
        (
            [
                # invalid as 0 is imputed to None first, then None is imputed to 0 (double imputation)
                {"type": "not_in", "expected_values": [-999, -99], "imputed_value": None},
                {"type": "missing", "imputed_value": 0},
            ],
            ["UnexpectedValueImputation", "MissingValueImputation"],
        ),
        (
            [
                # invalid as None is imputed to 0 first, then 0 is imputed to 1 (double imputation)
                {"type": "missing", "imputed_value": 0},
                {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
                {"type": "less_than_or_equal", "end_point": 0, "imputed_value": 1},
            ],
            ["MissingValueImputation", "ValueBeyondEndpointImputation"],
        ),
    ],
)
def test_critical_data_info__invalid_imputations(imputations, conflicts):
    """Test invalid imputations exception raised properly"""
    with pytest.raises(ValidationError) as exc:
        CriticalDataInfo(cleaning_operations=imputations)
    error_msg = str(exc.value.json())
    expected_msg = "Please revise the imputations so that no value could be imputed twice."
    assert expected_msg in error_msg
    for conflict_imputation in conflicts:
        assert conflict_imputation in error_msg


@pytest.mark.parametrize(
    "imputation,expected_nodes",
    [
        (
            MissingValueImputation(imputed_value=0),
            [
                {"name": "is_null_1", "type": "is_null", "output_type": "series", "parameters": {}},
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": 0},
                },
            ],
        ),
        (
            DisguisedValueImputation(disguised_values=["a", "b"], imputed_value=None),
            [
                {
                    "name": "is_in_1",
                    "type": "is_in",
                    "output_type": "series",
                    "parameters": {"value": ["a", "b"]},
                },
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": None},
                },
            ],
        ),
        (
            UnexpectedValueImputation(expected_values=["c", "d"], imputed_value=None),
            [
                {
                    "name": "is_in_1",
                    "type": "is_in",
                    "output_type": "series",
                    "parameters": {"value": ["c", "d"]},
                },
                {
                    "name": "not_1",
                    "type": "not",
                    "output_type": "series",
                    "parameters": {},
                },
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": None},
                },
            ],
        ),
        (
            ValueBeyondEndpointImputation(type="greater_than", end_point=0, imputed_value=0),
            [
                {
                    "name": "gt_1",
                    "type": "gt",
                    "output_type": "series",
                    "parameters": {"value": 0},
                },
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": 0},
                },
            ],
        ),
        (
            StringValueImputation(imputed_value=None),
            [
                {
                    "name": "is_string_1",
                    "type": "is_string",
                    "output_type": "series",
                    "parameters": {},
                },
                {
                    "name": "conditional_1",
                    "type": "conditional",
                    "output_type": "series",
                    "parameters": {"value": None},
                },
            ],
        ),
    ],
)
def test_critical_data_info__add_cleaning_operation(input_node, imputation, expected_nodes):
    """Test add cleaning operation output graph"""
    graph_node, _ = GraphNode.create(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
        graph_node_type=GraphNodeType.CLEANING,
    )
    node_op = imputation.add_cleaning_operation(
        graph_node=graph_node,
        input_node=graph_node.output_node,
        dtype=DBVarType.FLOAT,
    )
    assert node_op.type == NodeType.CAST
    nested_graph = graph_node.parameters.graph
    assert nested_graph.nodes[0] == {
        "name": "proxy_input_1",
        "type": "proxy_input",
        "output_type": "frame",
        "parameters": {"input_order": 0},
    }
    assert nested_graph.nodes[1] == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["a"]},
    }
    end_idx = 2 + len(expected_nodes)
    assert nested_graph.nodes[2:end_idx] == expected_nodes
    assert nested_graph.nodes[end_idx] == {
        "name": "cast_1",
        "type": "cast",
        "output_type": "series",
        "parameters": {"type": "float", "from_dtype": "FLOAT"},
    }


@pytest.mark.parametrize(
    "cleaning_operation,expected_repr",
    [
        (MissingValueImputation(imputed_value=0), "MissingValueImputation(imputed_value=0)"),
        (
            DisguisedValueImputation(disguised_values=["a", "b"], imputed_value=None),
            "DisguisedValueImputation(imputed_value=None, disguised_values=['a', 'b'])",
        ),
        (
            UnexpectedValueImputation(expected_values=["c", "d"], imputed_value=None),
            "UnexpectedValueImputation(imputed_value=None, expected_values=['c', 'd'])",
        ),
        (
            # should include type in repr as there are multiple types
            ValueBeyondEndpointImputation(type="greater_than", end_point=0, imputed_value=0),
            "ValueBeyondEndpointImputation(imputed_value=0, type=greater_than, end_point=0)",
        ),
        (StringValueImputation(imputed_value=None), "StringValueImputation(imputed_value=None)"),
    ],
)
def test_cleaning_operation_representation(cleaning_operation, expected_repr):
    """Test cleaning operation representation"""
    assert repr(cleaning_operation) == expected_repr
