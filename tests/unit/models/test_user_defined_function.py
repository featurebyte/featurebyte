"""
Unit tests for the UserDefinedFunction model.
"""
import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.user_defined_function import UserDefinedFunctionModel


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
                    "has_default_value": False,
                    "has_test_value": False,
                },
                {
                    "name": "param2",
                    "dtype": "FLOAT",
                    "default_value": 1.0,
                    "test_value": 0.0,
                    "has_default_value": True,
                    "has_test_value": True,
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
        "has_default_value": True,
        "has_test_value": True,
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
