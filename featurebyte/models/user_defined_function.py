"""
This module contains UserDefinedFunction related models
"""
from __future__ import annotations

from typing import Any, List, Optional

from pydantic import validator

from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)


class FunctionParameter(FeatureByteBaseModel):
    """
    FunctionArg class

    name: str
        Name of the generic function parameter (used for documentation purpose)
    dtype: DBVarType
        Data type of the parameter
    default_value: Optional[Any]
        Default value of the parameter
    test_value: Optional[Any]
        Test value of the parameter
    has_default_value: bool
        Whether the parameter has default value
    has_test_value: bool
        Whether the parameter has test value
    """

    name: str
    dtype: DBVarType
    default_value: Optional[Any]
    test_value: Optional[Any]

    # attributes below are used for indicating whether the parameters have certain values
    has_default_value: bool
    has_test_value: bool


class UserDefinedFunctionModel(FeatureByteBaseDocumentModel):
    """
    UserDefinedFunction model stores user defined function information

    function_name: str
        Name of the function used to call in the SQL query
    function_parameters: List[FunctionParameter]
        List of function parameter specification
    catalog_id: Optional[PydanticObjectId]
        Catalog id of the function (if any), if not provided, it can be used across all catalogs
    """

    function_name: str
    function_parameters: List[FunctionParameter]
    catalog_id: Optional[PydanticObjectId]

    @validator("function_name")
    @classmethod
    def _validate_function_name(cls, value: str) -> str:
        # check that function name is a valid function name
        if not value.isidentifier():
            raise ValueError(f'Function name "{value}" is not valid')
        return value

    @validator("function_parameters")
    @classmethod
    def _validate_function_parameters(
        cls, value: List[FunctionParameter]
    ) -> List[FunctionParameter]:
        # check that function parameter name is unique and valid
        func_names = set()
        for func_param in value:
            if func_param.name in func_names:
                raise ValueError(f'Function parameter name "{func_param.name}" is not unique')
            if not func_param.name.isidentifier():
                raise ValueError(f'Function parameter name "{func_param.name}" is not valid')
            func_names.add(func_param.name)
        return value

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "user_defined_function"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]
