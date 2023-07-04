"""
UserDefinedFunction API object
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional

from http import HTTPStatus

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.user_defined_function import UserDefinedFunctionListHandler
from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.user_defined_function_injector import (
    FunctionAccessor,
    UserDefinedFunctionInjector,
)
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType
from featurebyte.exception import DocumentCreationError, InvalidSettingsError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, get_active_catalog_id
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.schema.user_defined_function import UserDefinedFunctionCreate

logger = get_logger(__name__)


def _get_active_feature_store_id(catalog_id: Optional[ObjectId] = None) -> Optional[ObjectId]:
    # get the active feature store id
    if catalog_id is None:
        catalog_id = get_active_catalog_id()

    client = Configurations().get_client()
    response = client.get(f"/catalog/{catalog_id}")
    if response.status_code == HTTPStatus.OK:
        default_feature_store_ids = response.json()["default_feature_store_ids"]
        if default_feature_store_ids:
            assert len(default_feature_store_ids) == 1, "Only one default feature store is allowed."
            return ObjectId(default_feature_store_ids[0])
    return None


def _synchronize_user_defined_function(
    func_accessor: FunctionAccessor, route: str, feature_store_id: Optional[ObjectId]
) -> None:
    # synchronize all user-defined functions to the function accessor
    name_to_udf_dict: dict[str, UserDefinedFunctionModel] = {}
    try:
        if feature_store_id is None:
            feature_store_id = _get_active_feature_store_id()
            if feature_store_id is None:
                # Cannot synchronize user-defined functions without specifying a feature store
                return

        for udf_dict in iterate_api_object_using_paginated_routes(
            route, params={"feature_store_id": feature_store_id}
        ):
            udf = UserDefinedFunctionModel(**udf_dict)
            assert udf.name is not None, "User-defined function name cannot be None."
            if udf.name in name_to_udf_dict and udf.catalog_id is None:
                # if udf is global but there is already a UDF with the same name (which must be local),
                # skip the global UDF as it is overridden by the local one
                continue
            name_to_udf_dict[udf.name] = udf
    except InvalidSettingsError:
        # ignore invalid settings error due to fail to connect to the server
        logger.info("Failed to synchronize user-defined functions.")

    # construct & add all user-defined functions to the function accessor
    for udf in name_to_udf_dict.values():
        UserDefinedFunctionInjector.create_and_add_function(func_accessor, udf)


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


class UserDefinedFunction(DeletableApiObject, SavableApiObject):
    """
    UserDefinedFunction class used to represent a UserDefinedFunction in FeatureByte.
    """

    # class variables
    _route = "/user_defined_function"
    _create_schema_class = UserDefinedFunctionCreate
    _get_schema = UserDefinedFunctionModel
    _list_schema = UserDefinedFunctionModel
    _list_fields = [
        "signature",
        "function_name",
        "feature_store_name",
        "is_global",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]

    # pydantic instance variable (internal use)
    internal_catalog_id: Optional[PydanticObjectId] = Field(alias="catalog_id")
    internal_feature_store_id: PydanticObjectId = Field(alias="feature_store_id")
    internal_function_name: str = Field(alias="function_name")
    internal_function_parameters: List[FunctionParameter] = Field(alias="function_parameters")
    internal_output_dtype: DBVarType = Field(alias="output_dtype")

    # function accessor containing all user-defined functions
    func: ClassVar[Any] = FunctionDescriptor(route=_route)

    @property
    def catalog_id(self) -> Optional[PydanticObjectId]:
        """
        Catalog id of the user-defined function. If the user-defined function is global, the catalog id is None.

        Returns
        -------
        Optional[PydanticObjectId]
        """
        return self.cached_model.catalog_id

    @property
    def feature_store_id(self) -> PydanticObjectId:
        """
        Associated feature store id of the user-defined function.

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.feature_store_id

    @property
    def function_name(self) -> str:
        """
        The SQL function name of the user-defined function (which is used in SQL queries).

        Returns
        -------
        str
        """
        return self.cached_model.function_name

    @property
    def function_parameters(self) -> List[FunctionParameter]:
        """
        The function parameters of the user-defined function.

        Returns
        -------
        List[FunctionParameter]
        """
        return self.cached_model.function_parameters

    @property
    def output_dtype(self) -> DBVarType:
        """
        The output data type of the user-defined function.

        Returns
        -------
        DBVarType
        """
        return self.cached_model.output_dtype

    @property
    def signature(self) -> str:
        """
        The signature of the user-defined function.

        Returns
        -------
        str
        """
        return self.cached_model.signature

    @property
    def is_global(self) -> bool:
        """
        Whether the user-defined function is global across all catalogs or not. Global user-defined functions
        can be used in any catalogs (as long as the feature store is the same), while local user-defined functions
        can only be used in the catalog where they are created.

        Returns
        -------
        bool
        """
        return self.catalog_id is None

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return UserDefinedFunctionListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        function_name: str,
        function_parameters: List[FunctionParameter],
        output_dtype: Literal[tuple(DBVarType)],  # type: ignore[misc]
        is_global: bool = False,
    ) -> UserDefinedFunction:
        """
        Create a user-defined function.

        Parameters
        ----------
        name: str
            Name of the user-defined function. This will be used as the name of the dynamically created
            function.
        function_name: str
            The SQL function name of the user-defined function (which is used in SQL queries).
        function_parameters: List[FunctionParameter]
            The function parameters of the user-defined function.
        output_dtype: Literal[tuple(DBVarType)]
            The output data type of the user-defined function.
        is_global: bool
            Whether the user-defined function is global across all catalogs or not. Global user-defined
            functions can be used in any catalogs (as long as the feature store is the same), while local
            user-defined functions can only be used in the catalog where they are created.

        Returns
        -------
        UserDefinedFunction
            The created user-defined function.

        Raises
        ------
        DocumentCreationError
            If the user-defined function cannot be created.
        """
        active_catalog_id = get_active_catalog_id()
        active_feature_store_id = _get_active_feature_store_id(catalog_id=active_catalog_id)
        if not active_feature_store_id:
            raise DocumentCreationError(
                "Current active catalog does not have a default feature store. "
                "Please activate a catalog with a default feature store first before "
                "creating a user-defined function."
            )

        user_defined_function = UserDefinedFunction(
            name=name,
            function_name=function_name,
            function_parameters=function_parameters,
            output_dtype=output_dtype,
            catalog_id=None if is_global else active_catalog_id,
            feature_store_id=active_feature_store_id,
        )
        user_defined_function.save()
        return user_defined_function

    def delete(self) -> None:
        """
        Delete the user-defined function from the persistent data store. The user-defined function can only be
        deleted if it is not used by any saved feature.
        """
        super()._delete()


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
