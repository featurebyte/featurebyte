"""Test request column node related functionality."""

import pytest

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.metadata.config import SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationContext,
    VariableNameGenerator,
)
from featurebyte.query_graph.node.request import RequestColumnNode


@pytest.mark.parametrize(
    "dtype_data",
    [
        # old graph
        {"dtype": DBVarType.VARCHAR},
        # new graph that breaks old client
        {"dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR)},
        {"dtype_info": {"dtype": DBVarType.VARCHAR, "metadata": None}},
        {
            # new graph that does not break old client
            "dtype_info": DBVarTypeInfo(dtype=DBVarType.VARCHAR, metadata=None),
            "dtype": DBVarType.VARCHAR,
        },
    ],
)
def test_request_column_node_backward_compatibility(dtype_data):
    """
    Test backward compatibility of RequestColumnNode
    """
    node_dict = {
        "name": "request_column_node",
        "output_type": NodeOutputType.SERIES,
        "parameters": {
            "column_name": "request_column",
        },
    }
    node_dict["parameters"].update(dtype_data)
    node = RequestColumnNode(**node_dict)

    # check that old way of specifying dtype works
    assert node.parameters.dtype == DBVarType.VARCHAR

    # check that dtype_info is set correctly
    assert node.parameters.dtype_info == DBVarTypeInfo(dtype=DBVarType.VARCHAR)


def test_request_column_node_sdk_code_point_in_time():
    """
    Test SDK code generation for POINT_IN_TIME request column
    """
    node = RequestColumnNode(
        name="request_column_1",
        output_type=NodeOutputType.SERIES,
        parameters={
            "column_name": SpecialColumnName.POINT_IN_TIME,
            "dtype": DBVarType.TIMESTAMP,
        },
    )
    statements, var_name_or_expr = node.derive_sdk_code(
        node_inputs=[],
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            columns=[],
            aggregations=[],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(node.name,),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    assert len(statements) == 1
    _, obj = statements[0]
    assert str(obj) == "RequestColumn.point_in_time()"


SAMPLE_CONTEXT_ID = "6471a3d0f2b3c8a1e9d5f012"


@pytest.mark.parametrize(
    "column_name, dtype",
    [
        ("annual_income", DBVarType.FLOAT),
        ("credit_score", DBVarType.INT),
        ("customer_name", DBVarType.VARCHAR),
    ],
)
def test_request_column_node_sdk_code_user_provided_column(column_name, dtype):
    """
    Test SDK code generation for user-provided (non-POINT_IN_TIME) request columns
    generates Context.get_by_id(...).get_user_provided_feature(...) calls
    """
    node = RequestColumnNode(
        name="request_column_1",
        output_type=NodeOutputType.SERIES,
        parameters={
            "column_name": column_name,
            "dtype": dtype,
            "context_id": SAMPLE_CONTEXT_ID,
        },
    )
    statements, var_name_or_expr = node.derive_sdk_code(
        node_inputs=[],
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            columns=[],
            aggregations=[],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(node.name,),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    # Should produce two statements:
    # 1. context = Context.get_by_id(ObjectId("..."))
    # 2. request_col = context.get_user_provided_feature(column_name="...")
    assert len(statements) == 2
    context_var, context_obj = statements[0]
    assert str(context_var) == "context"
    assert str(context_obj) == f'Context.get_by_id(ObjectId("{SAMPLE_CONTEXT_ID}"))'

    var_name, feature_expr = statements[1]
    assert str(var_name) == "request_feature"
    assert str(feature_expr) == f'context.get_user_provided_feature(column_name="{column_name}")'
    assert str(var_name_or_expr) == "request_feature"


def test_request_column_node_sdk_code_user_provided_column_imports():
    """
    Test that SDK code generation for user-provided columns produces correct import info
    """
    node = RequestColumnNode(
        name="request_column_1",
        output_type=NodeOutputType.SERIES,
        parameters={
            "column_name": "annual_income",
            "dtype": DBVarType.FLOAT,
            "context_id": SAMPLE_CONTEXT_ID,
        },
    )
    statements, _ = node.derive_sdk_code(
        node_inputs=[],
        var_name_generator=VariableNameGenerator(),
        operation_structure=OperationStructure(
            columns=[],
            aggregations=[],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(node.name,),
        ),
        config=SDKCodeGenConfig(),
        context=CodeGenerationContext(as_info_dict=False, required_copy=False),
    )
    # First statement (Context.get_by_id) should require importing Context and ObjectId
    _, context_obj = statements[0]
    imports = context_obj.extract_import()
    assert ("featurebyte", "Context") in imports
    assert ("bson", "ObjectId") in imports


def test_request_column_node_context_id_backward_compatibility():
    """
    Test that RequestColumnNode without context_id still works (backward compatibility)
    """
    node = RequestColumnNode(
        name="request_column_1",
        output_type=NodeOutputType.SERIES,
        parameters={
            "column_name": "some_column",
            "dtype": DBVarType.FLOAT,
        },
    )
    assert node.parameters.context_id is None
