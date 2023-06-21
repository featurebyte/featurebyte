"""
Unit tests for the UserDefinedFunction class
"""
import pytest

from featurebyte.api.user_defined_function import UserDefinedFunction
from featurebyte.enum import DBVarType
from featurebyte.models.user_defined_function import FunctionParameter


@pytest.mark.parametrize(
    "function_parameters,is_global",
    [
        ([], True),
        ([], False),
        (
            [
                FunctionParameter(
                    name="x", dtype=DBVarType.INT, has_default_value=False, has_test_value=False
                )
            ],
            True,
        ),
    ],
)
def test_user_defined_function__create(function_parameters, is_global):
    """Test UserDefinedFunction save method"""
    user_defined_function = UserDefinedFunction.create(
        name="cos_func",
        function_name="cos",
        function_parameters=function_parameters,
        output_dtype=DBVarType.FLOAT,
        is_global=is_global,
    )
    assert user_defined_function.name == "cos_func"
    assert user_defined_function.function_name == "cos"
    assert user_defined_function.is_global == is_global
    assert user_defined_function.function_parameters == function_parameters
