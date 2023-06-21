"""
Unit tests for the UserDefinedFunction model.
"""
import pytest
from bson import ObjectId

from featurebyte.models.user_defined_function import UserDefinedFunctionModel


@pytest.mark.parametrize(
    "function_parameters,catalog_id",
    [
        ([], None),
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
        ),
    ],
)
def test_user_defined_function_model(function_parameters, catalog_id):
    """Test UserDefinedFunctionModel"""
    user_defined_function = UserDefinedFunctionModel(
        function_name="function_name",
        function_parameters=function_parameters,
        catalog_id=catalog_id,
    )
    assert user_defined_function.function_name == "function_name"
    assert user_defined_function.function_parameters == function_parameters
    assert user_defined_function.catalog_id == catalog_id


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
        )
    expected_msg = 'Function parameter name "param" is not unique'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            function_name="function_name",
            function_parameters=[{**func_param, "name": "invalid param name"}],
        )
    expected_msg = 'Function parameter name "invalid param name" is not valid'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        UserDefinedFunctionModel(
            function_name="invalid function name",
            function_parameters=[func_param],
        )
    expected_msg = 'Function name "invalid function name" is not valid'
    assert expected_msg in str(exc.value)
