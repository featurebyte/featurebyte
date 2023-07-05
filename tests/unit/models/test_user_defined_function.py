"""
Unit tests for the UserDefinedFunction model.
"""
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.user_defined_function import (
    FunctionParameter,
    UserDefinedFunctionModel,
    function_parameter_dtype_to_python_type,
    get_default_test_value,
)


@pytest.mark.parametrize(
    "dtype, default_value, test_value, expected_error_message",
    [
        (DBVarType.BOOL, None, None, None),
        (DBVarType.BOOL, False, False, None),
        (DBVarType.VARCHAR, "default", "test", None),
        (DBVarType.FLOAT, 1, 0, None),
        (DBVarType.FLOAT, 1.0, 0.0, None),
        (DBVarType.INT, 1, 0, None),
        (DBVarType.TIMESTAMP, pd.Timestamp.now(), pd.Timestamp.now(), None),
        (DBVarType.TIMESTAMP_TZ, pd.Timestamp.now(), pd.Timestamp.now(), None),
        (DBVarType.INT, "value", 0.0, "type of default_value must be int; got str instead"),
        (DBVarType.INT, 1, "value", "type of test_value must be int; got str instead"),
        (DBVarType.INT, 1.1, 0, "type of default_value must be int; got float instead"),
        (
            DBVarType.VOID,
            None,
            None,
            "Unsupported dtype: VOID, supported dtypes: [BOOL, VARCHAR, FLOAT, INT, TIMESTAMP, TIMESTAMP_TZ]",
        ),
    ],
)
def test_function_parameter(dtype, default_value, test_value, expected_error_message):
    """Test function parameter constructor"""
    if expected_error_message:
        with pytest.raises(TypeError) as exc:
            FunctionParameter(
                name="x", dtype=dtype, default_value=default_value, test_value=test_value
            )
        assert expected_error_message in str(exc.value)
    else:
        FunctionParameter(name="x", dtype=dtype, default_value=default_value, test_value=test_value)


@pytest.mark.parametrize(
    "function_parameters,catalog_id,expected_signature,expected_test_sql",
    [
        ([], None, "function_name() -> float", "SELECT SQL_FUNC()"),
        (
            [
                {
                    "name": "param1",
                    "dtype": "INT",
                    "default_value": None,
                    "test_value": None,
                },
                {
                    "name": "param2",
                    "dtype": "FLOAT",
                    "default_value": 1.0,
                    "test_value": 0.0,
                },
            ],
            ObjectId(),
            "function_name(param1: int, param2: float = 1.0) -> float",
            "SELECT SQL_FUNC(1, 0.0)",
        ),
    ],
)
def test_user_defined_function_model(
    function_parameters, catalog_id, expected_signature, expected_test_sql
):
    """Test UserDefinedFunctionModel"""
    feature_store_id = ObjectId()
    user_defined_function = UserDefinedFunctionModel(
        name="function_name",
        sql_function_name="sql_func",
        function_parameters=function_parameters,
        output_dtype=DBVarType.FLOAT,
        catalog_id=catalog_id,
        feature_store_id=feature_store_id,
    )
    assert user_defined_function.sql_function_name == "sql_func"
    assert user_defined_function.function_parameters == function_parameters
    assert user_defined_function.catalog_id == catalog_id
    assert user_defined_function.output_dtype == DBVarType.FLOAT
    assert user_defined_function.signature == expected_signature
    assert user_defined_function.feature_store_id == feature_store_id

    test_sql = user_defined_function.generate_test_sql(source_type=SourceType.SNOWFLAKE)
    assert test_sql == expected_test_sql


def test_user_defined_function_model__validator():
    """Test UserDefinedFunctionModel validator"""
    func_param = {
        "name": "param",
        "dtype": "FLOAT",
        "default_value": 1.0,
        "test_value": 0.0,
    }
    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            function_name="function_name",
            function_parameters=[func_param, func_param],
            output_dtype=DBVarType.FLOAT,
        )
    expected_msg = 'Function parameter name "param" is not unique'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            function_name="function_name",
            function_parameters=[{**func_param, "name": "invalid param name"}],
            output_dtype=DBVarType.FLOAT,
        )
    expected_msg = 'Function parameter name "invalid param name" is not valid'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            sql_function_name="invalid function name",
            function_parameters=[func_param],
            output_dtype=DBVarType.FLOAT,
        )
    expected_msg = '"invalid function name" is not a valid identifier'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            name="invalid name",
            sql_function_name="function_name",
            function_parameters=[func_param],
            output_dtype=DBVarType.FLOAT,
            feature_store_id=ObjectId(),
        )
    expected_msg = '"invalid name" is not a valid identifier'
    assert expected_msg in str(exc.value)


@pytest.mark.parametrize("dtype,expected_type", function_parameter_dtype_to_python_type.items())
def test_get_default_test_value(dtype, expected_type):
    """Test that supported dtypes return a valid default test value"""
    value = get_default_test_value(dtype)
    assert isinstance(value, expected_type)


@pytest.mark.parametrize(
    "dtype,expected_sql",
    [
        (DBVarType.BOOL, "SELECT SQL_FUNC(FALSE)"),
        (DBVarType.VARCHAR, "SELECT SQL_FUNC('test')"),
        (DBVarType.FLOAT, "SELECT SQL_FUNC(1.0)"),
        (DBVarType.INT, "SELECT SQL_FUNC(1)"),
        (DBVarType.TIMESTAMP, "SELECT SQL_FUNC(TO_TIMESTAMP('2021-01-01T00:00:00'))"),
        (DBVarType.TIMESTAMP_TZ, "SELECT SQL_FUNC(TO_TIMESTAMP('2021-01-01T00:00:00'))"),
    ],
)
def test_generate_test_sql(dtype, expected_sql):
    """Test generate_test_sql method"""
    function_parameter = FunctionParameter(name="x", dtype=dtype)
    udf = UserDefinedFunctionModel(
        name="function_name",
        sql_function_name="sql_func",
        function_parameters=[function_parameter],
        output_dtype=DBVarType.FLOAT,
        feature_store_id=ObjectId(),
    )
    assert udf.generate_test_sql(source_type=SourceType.SNOWFLAKE) == expected_sql
