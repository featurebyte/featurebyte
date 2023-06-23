"""
UserDefinedFunction API object
"""
from __future__ import annotations

import types
from typing import Any, ClassVar, List, Literal, Optional, Tuple

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.user_defined_function import UserDefinedFunctionListHandler
from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.catalog import Catalog
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.view import ViewColumn
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, FunctionParameterInputForm
from featurebyte.exception import DocumentCreationError, InvalidSettingsError
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.base import PydanticObjectId, get_active_catalog_id
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.function import (
    FunctionParameterInput,
    GenericFunctionNodeParameters,
)
from featurebyte.schema.user_defined_function import UserDefinedFunctionCreate

logger = get_logger(__name__)


def _get_active_feature_store_id() -> Optional[ObjectId]:
    # get the active feature store id
    catalog = Catalog.get_by_id(get_active_catalog_id())
    default_feature_store_ids = catalog.default_feature_store_ids
    if default_feature_store_ids:
        assert (
            len(catalog.default_feature_store_ids) == 1
        ), "Only one default feature store is allowed."
        return default_feature_store_ids[0]
    return None


def _process_keyword_arguments(
    udf: UserDefinedFunctionModel, **kwargs: Any
) -> Tuple[List[FunctionParameterInput], List[str], FeatureStoreModel, TabularSource]:
    # extract input parameters based on the function parameter specification
    func_params = []
    series_inputs: List[Series] = []
    for param in udf.function_parameters:
        if param.name not in kwargs:
            if param.has_default_value:
                value = param.default_value
            else:
                raise ValueError(f"Parameter {param.name} is not provided")
        else:
            value = kwargs.pop(param.name)

        if isinstance(value, Series):
            input_form = FunctionParameterInputForm.COLUMN
            series_inputs.append(value)
            column_name = value.name
        else:
            input_form = FunctionParameterInputForm.VALUE
            column_name = None

        func_params.append(
            FunctionParameterInput(
                value=value,
                dtype=param.dtype,
                input_form=input_form,
                column_name=column_name,
            )
        )

    # handle the remaining parameters
    if kwargs:
        raise ValueError(f"Unknown parameters {kwargs}")

    if not series_inputs:
        raise ValueError("At least one parameter must be a series.")

    series_input = series_inputs[0]
    input_node_names = [series_input.node_name for series_input in series_inputs]
    return func_params, input_node_names, series_input.feature_store, series_input.tabular_source


def _register_user_defined_function(
    func_accessor: FunctionAccessor, udf: UserDefinedFunctionModel
) -> None:
    # register a user-defined function to the function accessor.

    def method_wrapper(obj, **kwargs: Any):
        _ = obj
        func_params, input_node_names, feature_store, tabular_source = _process_keyword_arguments(
            udf, **kwargs
        )
        node_params = GenericFunctionNodeParameters(
            function_name=udf.function_name,
            function_parameters=func_params,
            output_dtype=udf.output_dtype,
            function_id=udf.id,
        )
        graph = GlobalQueryGraph()
        input_nodes = [
            graph.get_node_by_name(input_node_name) for input_node_name in input_node_names
        ]
        node = graph.add_operation(
            node_type=NodeType.GENERIC_FUNCTION,
            node_params=node_params.dict(by_alias=True),
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )

        return ViewColumn(
            name=None,
            node_name=node.name,
            feature_store=feature_store,
            tabular_source=tabular_source,
            dtype=udf.output_dtype,
        )

    # assign the dynamic method to the function accessor
    dynamic_func = types.MethodType(method_wrapper, func_accessor)
    setattr(func_accessor, udf.name, dynamic_func)


def _synchronize_user_defined_function(
    func_accessor: FunctionAccessor, route: str, feature_store_id: Optional[PydanticObjectId]
):
    if feature_store_id is None:
        active_catalog_id = get_active_catalog_id()
        catalog = Catalog.get_by_id(active_catalog_id)
        if len(catalog.default_feature_store_ids) != 1:
            logger.info(
                "Cannot synchronize user-defined functions without specifying a feature store"
            )
            return

        feature_store_id = catalog.default_feature_store_ids[0]

    # synchronize all user-defined functions to the function accessor
    try:
        for udf_dict in iterate_api_object_using_paginated_routes(
            route, params={"feature_store_id": feature_store_id}
        ):
            udf = UserDefinedFunctionModel(**udf_dict)
            _register_user_defined_function(func_accessor, udf)
    except InvalidSettingsError:
        # ignore invalid settings error due to fail to connect to the server
        logger.info("Failed to synchronize user-defined functions.")


class FunctionAccessor:
    """
    FunctionAccessor class contains all user-defined functions as its methods.
    Note: Do not add any methods to this class. All methods are dynamically added by the FunctionDescriptor class.
    """


class FunctionDescriptor:
    """
    FunctionDescriptor class used to perform dynamic function registration when the descriptor is accessed.
    """

    def __init__(self, route: str):
        self._route = route

    def __get__(self, instance: object, owner: type[object]) -> FunctionAccessor:
        # create a function accessor object as a user-defined function container and
        # register all user-defined functions to the function accessor
        accessor = FunctionAccessor()
        _synchronize_user_defined_function(
            func_accessor=accessor, route=self._route, feature_store_id=None
        )
        return accessor


class UserDefinedFunction(SavableApiObject):
    """
    UserDefinedFunction class used to represent a UserDefinedFunction in FeatureByte.
    """

    # class variables
    _route = "/user_defined_function"
    _create_schema_class = UserDefinedFunctionCreate
    _get_schema = UserDefinedFunctionModel
    _list_schema = UserDefinedFunctionModel
    _list_fields = ["name", "signature", "feature_store_name", "is_global", "active"]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]

    # pydantic instance variable (internal use)
    internal_output_dtype: DBVarType = Field(alias="output_dtype")
    internal_function_name: str = Field(alias="function_name")
    internal_function_parameters: List[FunctionParameter] = Field(alias="function_parameters")
    internal_catalog_id: Optional[PydanticObjectId] = Field(alias="catalog_id")
    internal_feature_store_id: PydanticObjectId = Field(alias="feature_store_id")

    # function accessor containing all user-defined functions
    func: ClassVar[Any] = FunctionDescriptor(route=_route)

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
        active_catalog_id = get_active_catalog_id()
        catalog = Catalog.get_by_id(active_catalog_id)
        default_feature_store_ids = catalog.default_feature_store_ids
        if not default_feature_store_ids:
            raise DocumentCreationError("No default feature store is set.")
        elif len(default_feature_store_ids) > 1:
            raise DocumentCreationError("Multiple default feature stores not supported.")

        user_defined_function = UserDefinedFunction(
            name=name,
            function_name=function_name,
            function_parameters=function_parameters,
            output_dtype=output_dtype,
            catalog_id=None if is_global else active_catalog_id,
            feature_store_id=default_feature_store_ids[0],
        )
        user_defined_function.save()

        # update the function accessor
        _synchronize_user_defined_function(
            func_accessor=cls.func,
            route=cls._route,
            feature_store_id=default_feature_store_ids[0],
        )
        return user_defined_function

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return UserDefinedFunctionListHandler(
            route=cls._route,
            active_feature_store_id=_get_active_feature_store_id(),
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )


class UserDefinedFunctionAccessorWrapper:
    """
    UserDefinedFunctionWrapper class used to access user-defined functions dynamically.
    The FunctionAccessor object is constructed dynamically when needed.
    """

    def __dir__(self) -> List[str]:
        # provide auto-completion for user-defined functions by dynamically constructing the function accessor
        # and returning the function accessor's dir list
        return dir(UserDefinedFunction.func)

    def __getattr__(self, item: str) -> Any:
        # provide auto-completion for user-defined functions by dynamically constructing the function accessor
        # and returning the function accessor's attribute
        return getattr(UserDefinedFunction.func, item)


# create a global instance of the user-defined function accessor handler
UDF = UserDefinedFunctionAccessorWrapper()
