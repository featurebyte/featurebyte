"""
UserDefinedFunction API object
"""
from __future__ import annotations

import types
from typing import Any, ClassVar, Dict, List, Literal, Optional

from pandas import DataFrame
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object_util import iterate_api_object_using_paginated_routes
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId, get_active_catalog_id
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.schema.user_defined_function import UserDefinedFunctionCreate


def _register_user_defined_function(
    func_accessor: FunctionAccessor, udf: UserDefinedFunctionModel
) -> None:
    # register a user-defined function to the function accessor.

    def _process_keyword_arguments(**kwargs: Any) -> Dict[str, Any]:
        # extract input parameters based on the function parameter specification
        parameters = {}
        for param in udf.function_parameters:
            if param.name not in kwargs:
                if param.has_default_value:
                    parameters[param.name] = param.default_value
                else:
                    raise ValueError(f"Parameter {param.name} is not provided")
            else:
                parameters[param.name] = kwargs.pop(param.name)

        # handle the remaining parameters
        if kwargs:
            raise ValueError(f"Unknown parameters {kwargs}")
        return parameters

    def dynamic_method(obj, **kwargs: Any):
        _ = obj
        parameters = _process_keyword_arguments(**kwargs)
        print(f"Calling {udf.function_name} with parameters {parameters}")

    # assign the dynamic method to the function accessor
    dynamic_func = types.MethodType(dynamic_method, func_accessor)
    setattr(func_accessor, udf.name, dynamic_func)


def _synchronize_user_defined_function(func_accessor: FunctionAccessor, route: str):
    # synchronize all user-defined functions to the function accessor
    for udf_dict in iterate_api_object_using_paginated_routes(route):
        udf = UserDefinedFunctionModel(**udf_dict)
        _register_user_defined_function(func_accessor, udf)


class FunctionAccessor:
    """
    FunctionAccessor class used to access user-defined functions stored in FeatureByte.
    """

    def __init__(self, route: str):
        self._route = route
        _synchronize_user_defined_function(func_accessor=self, route=route)


class UserDefinedFunction(SavableApiObject):
    """
    UserDefinedFunction class used to represent a UserDefinedFunction in FeatureByte.
    """

    # class variables
    _route = "/user_defined_function"
    _create_schema_class = UserDefinedFunctionCreate
    _get_schema = UserDefinedFunctionModel
    _list_schema = UserDefinedFunctionModel
    _list_fields = ["name", "signature", "is_global"]

    # pydantic instance variable (internal use)
    internal_output_dtype: DBVarType = Field(alias="output_dtype")
    internal_function_name: str = Field(alias="function_name")
    internal_function_parameters: List[FunctionParameter] = Field(alias="function_parameters")
    internal_catalog_id: Optional[PydanticObjectId] = Field(alias="catalog_id")

    # function accessor with containing all user-defined functions
    func: ClassVar[Any] = FunctionAccessor(route=_route)

    @property
    def catalog_id(self) -> Optional[PydanticObjectId]:
        return self.cached_model.catalog_id

    @property
    def function_name(self) -> str:
        return self.cached_model.function_name

    @property
    def function_parameters(self) -> List[FunctionParameter]:
        return self.cached_model.function_parameters

    @property
    def output_dtype(self) -> DBVarType:
        return self.cached_model.output_dtype

    @property
    def is_global(self) -> bool:
        return self.catalog_id is None

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        function_name: str,
        function_parameters: List[FunctionParameter],
        output_dtype: Literal[tuple(DBVarType)],
        is_global: bool = False,
    ) -> UserDefinedFunction:
        user_defined_function = UserDefinedFunction(
            name=name,
            function_name=function_name,
            function_parameters=function_parameters,
            output_dtype=output_dtype,
            catalog_id=None if is_global else get_active_catalog_id(),
        )
        user_defined_function.save()

        # update the function accessor
        _register_user_defined_function(
            func_accessor=cls.func, udf=user_defined_function.cached_model
        )
        return user_defined_function

    @classmethod
    def _post_process_list(cls, item_list: DataFrame) -> DataFrame:
        output = super()._post_process_list(item_list)
        output["is_global"] = output["catalog_id"].isnull()
        return output
