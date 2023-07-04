"""
Unit tests for the UserDefinedFunctionInjector class.
"""
from typing import Union

import inspect
import textwrap

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.api.feature import Feature
from featurebyte.api.user_defined_function_injector import (
    FunctionAccessor,
    FunctionParameterProcessor,
    UserDefinedFunctionInjector,
)
from featurebyte.api.view import ViewColumn
from featurebyte.enum import DBVarType
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.query_graph.node.function import (
    ColumnFunctionParameterInput,
    ValueFunctionParameterInput,
)
from featurebyte.query_graph.node.scalar import TimestampValue


@pytest.fixture(name="function_parameters")
def function_parameters_fixture():
    """Fixture for function parameters"""
    return [
        FunctionParameter(
            name="x",
            dtype=DBVarType.INT,
            default_value=None,
            test_value=None,
            has_default_value=False,
            has_test_value=False,
        ),
        FunctionParameter(
            name="y",
            dtype=DBVarType.FLOAT,
            default_value=1.0,
            test_value=0.0,
            has_default_value=True,
            has_test_value=True,
        ),
    ]


@pytest.fixture(name="ts_function_parameters")
def ts_function_parameters_fixture():
    """Timestamp function parameters fixture"""
    common_kwargs = {
        "default_value": None,
        "test_value": None,
        "has_default_value": False,
        "has_test_value": False,
    }
    return [
        FunctionParameter(name="end_date", dtype=DBVarType.TIMESTAMP_TZ, **common_kwargs),
        FunctionParameter(name="start_date", dtype=DBVarType.TIMESTAMP_TZ, **common_kwargs),
    ]


@pytest.fixture(name="user_defined_function")
def user_defined_function_fixture(function_parameters):
    """Fixture for UserDefinedFunction"""
    return UserDefinedFunctionModel(
        name="udf_func",
        function_name="function_name",
        function_parameters=function_parameters,
        output_dtype=DBVarType.FLOAT,
        feature_store_id=ObjectId(),
        catalog_id=ObjectId(),
    )


def test_function_parameter_processor__extract_node_parameters__args_kwargs_validation(
    function_parameters,
):
    """Test FunctionParameterProcessor._extract_node_parameters args & kwargs validation"""
    function_parameter_processor = FunctionParameterProcessor(function_parameters)

    # check too many positional arguments
    with pytest.raises(ValueError) as exc:
        function_parameter_processor._extract_node_parameters(1, 2, 3)
    assert "Too many arguments. Expected 2 but got 3." in str(exc.value)

    # check missing required argument
    with pytest.raises(ValueError) as exc:
        function_parameter_processor._extract_node_parameters(y=1)
    assert 'Parameter "x" is not provided.' in str(exc.value)

    # check unknown keyword argument
    with pytest.raises(ValueError) as exc:
        function_parameter_processor._extract_node_parameters(1, 2, z=2)
    assert "Unknown keyword arguments: ['z']" in str(exc.value)


def test_function_parameter_processor__extract_node_parameters__input_validation(
    function_parameters, snowflake_event_view, snowflake_item_view, float_feature
):
    """Test FunctionParameterProcessor._extract_node_parameters input validation"""
    function_parameter_processor = FunctionParameterProcessor(function_parameters)

    # check invalid scalar type
    with pytest.raises(TypeError) as exc:
        function_parameter_processor._extract_node_parameters("a", 2)
    expected_error = (
        "type of x must be one of (int, featurebyte.api.view.ViewColumn, featurebyte.api.feature.Feature); "
        "got str instead"
    )
    assert expected_error in str(exc.value)

    # check invalid view column type
    with pytest.raises(TypeError) as exc:
        function_parameter_processor._extract_node_parameters(1, snowflake_event_view.col_char)
    assert 'Input ViewColumn or Feature "col_char" has dtype CHAR but expected FLOAT.' in str(
        exc.value
    )

    # check mis-matched series type
    with pytest.raises(TypeError) as exc:
        function_parameter_processor._extract_node_parameters(
            snowflake_event_view.col_int, float_feature
        )
    assert 'Input "sum_1d" has type Feature but expected ViewColumn.' in str(exc.value)

    # check missing series type
    with pytest.raises(ValueError) as exc:
        function_parameter_processor._extract_node_parameters(1, 2)
    assert "At least one parameter must be a ViewColumn or Feature." in str(exc.value)

    # check mis-matched row index lineage
    with pytest.raises(ValueError) as exc:
        function_parameter_processor._extract_node_parameters(
            snowflake_event_view.col_int, snowflake_item_view.item_amount
        )
    expected_error = (
        'The row of the input ViewColumns "col_int" does not match the row of the '
        'input ViewColumns "item_amount".'
    )
    assert expected_error in str(exc.value)


def test_function_parameter_processor_process(
    function_parameters, snowflake_event_view, float_feature
):
    """Test FunctionParameterProcessor.process"""
    function_parameter_processor = FunctionParameterProcessor(function_parameters)

    # check view column + scalar
    output = function_parameter_processor.process(snowflake_event_view.col_int, 2)
    assert output.node_function_parameters == [
        ColumnFunctionParameterInput(column_name="col_int", dtype=DBVarType.INT),
        ValueFunctionParameterInput(value=2, dtype="FLOAT"),
    ]
    assert output.input_node_names == [snowflake_event_view.col_int.node_name]
    assert output.feature_store == snowflake_event_view.feature_store
    assert output.tabular_source == snowflake_event_view.tabular_source
    assert output.output_type is ViewColumn

    # check feature + scalar
    output = function_parameter_processor.process(1, float_feature)
    assert output.node_function_parameters == [
        ValueFunctionParameterInput(value=1, dtype=DBVarType.INT),
        ColumnFunctionParameterInput(column_name="sum_1d", dtype=DBVarType.FLOAT),
    ]
    assert output.input_node_names == [float_feature.node_name]
    assert output.feature_store == float_feature.feature_store
    assert output.tabular_source == float_feature.tabular_source
    assert output.output_type is Feature

    # check default value
    output = function_parameter_processor.process(snowflake_event_view.col_int)
    assert output.node_function_parameters == [
        ColumnFunctionParameterInput(column_name="col_int", dtype=DBVarType.INT),
        ValueFunctionParameterInput(value=1, dtype=DBVarType.FLOAT),
    ]
    assert output.output_type is ViewColumn


def test_function_parameter_processor_process__timestamp_input(
    ts_function_parameters,
    snowflake_event_view,
):
    """Test FunctionParameterProcessor.process with timestamp input"""
    function_parameter_processor = FunctionParameterProcessor(ts_function_parameters)
    output = function_parameter_processor.process(
        pd.Timestamp("2020-01-01"),
        snowflake_event_view.event_timestamp,
    )
    assert output.node_function_parameters == [
        ValueFunctionParameterInput(
            value=TimestampValue(iso_format_str="2020-01-01T00:00:00"),
            dtype=DBVarType.TIMESTAMP_TZ,
        ),
        ColumnFunctionParameterInput(column_name="event_timestamp", dtype=DBVarType.TIMESTAMP_TZ),
    ]


def test_user_defined_function_injector_create_and_add_function(user_defined_function):
    """Test UserDefinedFunctionInjector.create_and_add_function"""
    accessor = FunctionAccessor()
    assert not hasattr(accessor, user_defined_function.name)

    # create function & add it to the accessor
    UserDefinedFunctionInjector.create_and_add_function(
        func_accessor=accessor, udf=user_defined_function
    )
    method = accessor.udf_func
    assert callable(method)

    # check repr
    assert repr(method).startswith("<function UDF.udf_func at")

    # check signature
    signature = inspect.signature(method)
    assert list(signature.parameters.values()) == [
        inspect.Parameter(
            "x", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Union[int, ViewColumn, Feature]
        ),
        inspect.Parameter(
            "y",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Union[float, ViewColumn, Feature],
        ),
    ]

    # check docstring
    expected_doc = """
    This function uses the feature store SQL function `function_name`
    to generate a new column or new feature. Note that the non-scalar input parameters
    must be homogeneous, i.e., they must be either all view columns or all features. The output
    type is determined by the input type. If all input parameters are view columns, then the output
    type is a view column. If all input parameters are features, then the output type is a feature.
    All view columns input parameters must have matching row alignment. Otherwise, an error is raised.

    Parameters
    ----------
    x : typing.Union[int, featurebyte.api.view.ViewColumn, featurebyte.api.feature.Feature]
    y : typing.Union[float, featurebyte.api.view.ViewColumn, featurebyte.api.feature.Feature]

    Returns
    -------
    Union[ViewColumn, Feature]
        A new column or new feature generated by the feature store function `function_name`.
    """
    assert method.__doc__ == textwrap.dedent(expected_doc).strip()
