"""
Tests for the SDK code generation.
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.generic import AliasNode, AssignNode, ConditionalNode
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    InfoDict,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
)


@pytest.mark.parametrize(
    "node_inputs, required_copy, expected_statements, expected_info",
    [
        # case 1: rename a column with required_copy=False
        (
            [VariableNameStr("col")],
            False,
            [(VariableNameStr("col.name"), ValueStr.create("new_col"))],
            VariableNameStr("col"),
        ),
        # case 2: rename an input with required_copy=False
        (
            [VariableNameStr("col.dt.year")],
            False,
            [
                (VariableNameStr("col_1"), ExpressionStr("col.dt.year")),
                (VariableNameStr("col_1.name"), ValueStr.create("new_col")),
            ],
            VariableNameStr("col_1"),
        ),
        # case 3: rename a column with required_copy=True
        (
            [VariableNameStr("col")],
            True,
            [
                (VariableNameStr("col_1"), ExpressionStr("col.copy()")),
                (VariableNameStr("col_1.name"), ValueStr.create("new_col")),
            ],
            VariableNameStr("col_1"),
        ),
        # case 4: rename an input with required_copy=True
        (
            [VariableNameStr("col.dt.year")],
            True,
            [
                (VariableNameStr("col_1"), ExpressionStr("col.dt.year.copy()")),
                (VariableNameStr("col_1.name"), ValueStr.create("new_col")),
            ],
            VariableNameStr("col_1"),
        ),
    ],
)
def test_alias_node(node_inputs, required_copy, expected_statements, expected_info):
    """Test AliasNode"""
    node = AliasNode(
        name="alias_1", parameters={"name": "new_col"}, output_type=NodeOutputType.SERIES
    )
    var_name_generator = VariableNameGenerator()
    # simulate a case where col has been defined
    var_name_generator.convert_to_variable_name("col", node_name=None)
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=var_name_generator,
        operation_structure=OperationStructure(
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=CodeGenerationConfig(),
        context=CodeGenerationContext(as_info_str=False, required_copy=required_copy),
    )
    assert statements == expected_statements
    assert info == expected_info


@pytest.mark.parametrize(
    "node_inputs, required_copy, expected_statements, expected_info",
    [
        # case 1: assign a constant value with required_copy=False
        (
            [VariableNameStr("event_view")],
            False,
            [(VariableNameStr("event_view['col']"), ValueStr.create(None))],
            VariableNameStr("event_view"),
        ),
        # case 2: assign a constant value with required_copy=True
        (
            [VariableNameStr("event_view")],
            True,
            [
                (VariableNameStr("view"), ExpressionStr("event_view.copy()")),
                (VariableNameStr("view['col']"), ValueStr.create(None)),
            ],
            VariableNameStr("view"),
        ),
        # case 3: conditional assign with required_copy=False
        (
            [
                VariableNameStr("event_view"),
                InfoDict({"value": 10, "mask": "col_1 > 10"}),
            ],
            False,
            [(VariableNameStr("event_view['col'][col_1 > 10]"), ValueStr.create(10))],
            VariableNameStr("event_view"),
        ),
        # case 4: conditional assign with required_copy=True
        (
            [
                VariableNameStr("event_view"),
                InfoDict({"value": 10, "mask": "col_1 > 10"}),
            ],
            True,
            [
                (VariableNameStr("view"), ExpressionStr("event_view.copy()")),
                (VariableNameStr("view['col'][col_1 > 10]"), ValueStr.create(10)),
            ],
            VariableNameStr("view"),
        ),
    ],
)
def test_assign_node(node_inputs, required_copy, expected_statements, expected_info):
    """Test AssignNode"""
    node = AssignNode(name="assign_1", parameters={"name": "col", "value": None})
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=CodeGenerationConfig(),
        context=CodeGenerationContext(as_info_str=False, required_copy=required_copy),
    )
    assert statements == expected_statements
    assert info == expected_info


@pytest.mark.parametrize(
    "node_inputs, required_copy, as_info_str, expected_statements, expected_info",
    [
        # case 1: conditional with required_copy=False, as_info_str=False, col mask
        (
            [
                VariableNameStr("col"),
                VariableNameStr("mask"),
            ],
            False,
            False,
            [(VariableNameStr("col[mask]"), ValueStr.create(1234))],
            VariableNameStr("col"),
        ),
        # case 2: conditional with required_copy=False, as_info_str=True, col mask
        (
            [
                VariableNameStr("col"),
                VariableNameStr("mask"),
            ],
            False,
            True,
            [],
            InfoDict({"value": 1234, "mask": "mask"}),
        ),
        # case 3: conditional with required_copy=False, as_info_str=False, expression mask
        (
            [
                VariableNameStr("col"),
                ExpressionStr("col > 10"),
            ],
            False,
            False,
            [
                (VariableNameStr("mask"), ExpressionStr("col > 10")),
                (VariableNameStr("col[mask]"), ValueStr.create(1234)),
            ],
            VariableNameStr("col"),
        ),
        # case 4: conditional with required_copy=False, as_info_str=True, expression mask
        (
            [
                VariableNameStr("col.dt.year"),
                ExpressionStr("col > 10"),
            ],
            False,
            True,
            [
                (VariableNameStr("mask"), ExpressionStr("col > 10")),
                (VariableNameStr("col_1"), ExpressionStr("col.dt.year")),
            ],
            InfoDict({"value": 1234, "mask": "mask"}),
        ),
        # case 5: conditional with required_copy=True, as_info_str=False, col mask
        (
            [
                VariableNameStr("col"),
                VariableNameStr("mask"),
            ],
            True,
            False,
            [
                (VariableNameStr("col_1"), ExpressionStr("col.copy()")),
                (VariableNameStr("col_1[mask]"), ValueStr.create(1234)),
            ],
            VariableNameStr("col_1"),
        ),
        # case 6: conditional with required_copy=True, as_info_str=True, expression mask
        (
            [
                VariableNameStr("col.dt.year"),
                ExpressionStr("col > 10"),
            ],
            True,
            True,
            [
                (VariableNameStr("mask"), ExpressionStr("col > 10")),
                (VariableNameStr("col_1"), ExpressionStr("col.dt.year")),
            ],
            InfoDict({"value": 1234, "mask": "mask"}),
        ),
    ],
)
def test_conditional(node_inputs, required_copy, as_info_str, expected_statements, expected_info):
    """Test ConditionalNode"""
    node = ConditionalNode(name="conditional_1", parameters={"value": 1234})
    output_type = (
        NodeOutputType.SERIES if node_inputs[0].startswith("col") else NodeOutputType.FRAME
    )
    var_name_generator = VariableNameGenerator()
    # simulate a case where col has been defined
    var_name_generator.convert_to_variable_name("col", node_name=None)
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=var_name_generator,
        operation_structure=OperationStructure(
            output_type=output_type,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=CodeGenerationConfig(),
        context=CodeGenerationContext(as_info_str=as_info_str, required_copy=required_copy),
    )
    assert statements == expected_statements
    assert info == expected_info
