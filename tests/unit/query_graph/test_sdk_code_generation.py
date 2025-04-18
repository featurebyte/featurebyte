"""
Tests for the SDK code generation.
"""

import ast
import textwrap

import pytest
from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.node.distance import HaversineNode
from featurebyte.query_graph.node.generic import (
    AliasNode,
    AssignNode,
    ConditionalNode,
    LookupTargetNode,
    LookupTargetParameters,
)
from featurebyte.query_graph.node.metadata.config import SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationContext,
    ExpressionStr,
    InfoDict,
    UnusedVariableFinder,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.query_graph.node.vector import VectorCosineSimilarityNode


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
def test_alias_node(
    node_inputs, required_copy, expected_statements, expected_info, node_code_gen_output_factory
):
    """Test AliasNode"""
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
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
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=required_copy),
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
def test_assign_node(
    node_inputs, required_copy, expected_statements, expected_info, node_code_gen_output_factory
):
    """Test AssignNode"""
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
    node = AssignNode(name="assign_1", parameters={"name": "col", "value": None})
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=required_copy),
    )
    assert statements == expected_statements
    assert info == expected_info


def test_vector_node(node_code_gen_output_factory):
    """
    Test vector cosine similarity node
    """
    node_inputs = [
        VariableNameStr("col1"),
        VariableNameStr("col2"),
    ]
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
    node = VectorCosineSimilarityNode(name="vector_cosine_similarity_1", parameters={})
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    assert statements == []
    assert info == "col1.vec.cosine_similarity(other=col2)"


def test_lookup_target_node(node_code_gen_output_factory):
    """
    Test lookup target node
    """
    lookup_target_node = LookupTargetNode(
        name="lookup_target",
        parameters=LookupTargetParameters(
            offset="7d",
            input_column_names=["target_col"],
            feature_names=["target"],
            entity_column="",
            serving_name="serving_name",
            entity_id=PydanticObjectId(ObjectId()),
        ),
    )
    statements, info = lookup_target_node.derive_sdk_code(
        node_inputs=[node_code_gen_output_factory(var_name_or_expr=VariableNameStr("view"))],
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.TARGET,
            row_index_lineage=tuple(),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    assert (
        info == 'view["target_col"].as_target(target_name="target", offset="7d", fill_value=None)'
    )
    assert statements == []


def test_haversine_node(node_code_gen_output_factory):
    """
    Test haversine node
    """
    node = HaversineNode(name="haversine_1", parameters={})
    node_inputs = [
        VariableNameStr("col1"),
        VariableNameStr("col2"),
        VariableNameStr("col3"),
        VariableNameStr("col4"),
    ]
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
    statements, info = node.derive_sdk_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=tuple(),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    assert len(statements) == 1
    assert statements[0][0] == "col"
    assert (
        str(statements[0][1])
        == "haversine(lat_series_1=col1, lon_series_1=col2, lat_series_2=col3, lon_series_2=col4)"
    )
    assert info == "col"


@pytest.mark.parametrize(
    "node_inputs, required_copy, as_info_dict, expected_statements, expected_info",
    [
        # case 1: conditional with required_copy=False, as_info_dict=False, col mask
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
        # case 2: conditional with required_copy=False, as_info_dict=True, col mask
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
        # case 3: conditional with required_copy=False, as_info_dict=False, expression mask
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
        # case 4: conditional with required_copy=False, as_info_dict=True, expression mask
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
        # case 5: conditional with required_copy=True, as_info_dict=False, col mask
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
        # case 6: conditional with required_copy=True, as_info_dict=True, expression mask
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
def test_conditional(
    node_inputs,
    required_copy,
    as_info_dict,
    expected_statements,
    expected_info,
    node_code_gen_output_factory,
):
    """Test ConditionalNode"""
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
    node = ConditionalNode(name="conditional_1", parameters={"value": 1234})
    output_type = (
        NodeOutputType.SERIES
        if node_inputs[0].var_name_or_expr.startswith("col")
        else NodeOutputType.FRAME
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
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=as_info_dict, required_copy=required_copy),
    )
    assert statements == expected_statements
    assert info == expected_info


@pytest.mark.parametrize(
    "code, expected_variables, expected_unused_variables",
    [
        (
            """
            x = 1
            y = 2
            print(x)
            """,
            {"x", "y"},
            {"y"},
        ),
        (
            """
            a = 1
            b = 2
            del a
            """,
            {"a", "b"},
            {"b"},
        ),
    ],
)
def test_unused_variable_finder(code, expected_variables, expected_unused_variables):
    """Test UnusedVariableFinder"""
    tree = ast.parse(textwrap.dedent(code))
    finder = UnusedVariableFinder()
    finder.visit(tree)
    assert finder.variables == expected_variables
    assert finder.get_unused_variables() == expected_unused_variables
