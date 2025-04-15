"""
UserDefinedFunction API object
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, ClassVar, Dict, List, Optional, Union, cast

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.user_defined_function import UserDefinedFunctionListHandler
from featurebyte.api.api_object_util import (
    ForeignKeyMapping,
    iterate_api_object_using_paginated_routes,
)
from featurebyte.api.feature import Feature
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.user_defined_function_injector import (
    FunctionAccessor,
    UserDefinedFunctionInjector,
)
from featurebyte.api.view import ViewColumn
from featurebyte.common import get_active_catalog_id
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType
from featurebyte.exception import InvalidSettingsError
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.user_defined_function import FunctionParameter, UserDefinedFunctionModel
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionResponse,
    UserDefinedFunctionUpdate,
)

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


def _synchronize_user_defined_function(func_accessor: FunctionAccessor, route: str) -> None:
    # synchronize all user-defined functions to the function accessor
    name_to_udf_dict: dict[str, UserDefinedFunctionModel] = {}
    try:
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
        _synchronize_user_defined_function(func_accessor=accessor, route=self._route)
        return accessor


class UserDefinedFunction(DeletableApiObject, SavableApiObject):
    """
    A UserDefinedFunction object represents a user-defined function that can be used to transform view columns
    or feature. This object is callable and can be used as a function to transform view columns or feature.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.UserDefinedFunction")
    _route: ClassVar[str] = "/user_defined_function"
    _create_schema_class: ClassVar[Any] = UserDefinedFunctionCreate
    _update_schema_class: ClassVar[Any] = UserDefinedFunctionUpdate
    _get_schema: ClassVar[Any] = UserDefinedFunctionResponse
    _list_schema: ClassVar[Any] = UserDefinedFunctionResponse
    _list_fields: ClassVar[List[str]] = [
        "signature",
        "sql_function_name",
        "feature_store_name",
        "is_global",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]

    # pydantic instance variable (internal use)
    internal_is_global: bool = Field(alias="is_global")
    internal_sql_function_name: str = Field(alias="sql_function_name")
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
    def sql_function_name(self) -> str:
        """
        The SQL function name of the user-defined function (which is used in SQL queries).

        Returns
        -------
        str
        """
        return self.cached_model.sql_function_name

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
        The signature of the user-defined function. Note that the function signature is not strictly a
        python function signature as the optional parameters can be at any position in the parameter list.
        For python function signature, the optional parameters must be at the end of the parameter list.

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
        return self.cached_model.is_global

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return UserDefinedFunctionListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    def _get(cls, name: str, other_params: Optional[dict[str, Any]] = None) -> UserDefinedFunction:
        # Override the default _get method to handle the case where there are multiple UDFs
        # with the same name (one global and one local). In this case, the local UDF should be returned.
        def _record_selector(records: list[dict[str, Any]]) -> dict[str, Any]:
            return sorted(records, key=lambda rec: rec["catalog_id"] is None)[0]

        return cls(
            **cls._get_object_dict_by_name(name=name, select_func=_record_selector),
            **cls._get_init_params(),
            _validate_schema=True,
        )

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        sql_function_name: str,
        function_parameters: List[FunctionParameter],
        output_dtype: Union[DBVarType, str],
        is_global: bool = False,
    ) -> UserDefinedFunction:
        """
        Create a user-defined function.

        Parameters
        ----------
        name: str
            Name of the user-defined function. This will be used as the name of the dynamically created
            function.
        sql_function_name: str
            The SQL function name of the user-defined function (which is used in SQL queries).
        function_parameters: List[FunctionParameter]
            The function parameters of the user-defined function.
        output_dtype: Union[DBVarType, str]
            The output data type of the user-defined function.
        is_global: bool
            Whether the user-defined function is global across all catalogs or not. Global user-defined
            functions can be used in any catalogs (as long as the feature store is the same), while local
            user-defined functions can only be used in the catalog where they are created.

        Returns
        -------
        UserDefinedFunction
            The created user-defined function.

        Examples
        --------
        Create a local (catalog-specific) user-defined function that computes the cosine of a number:

        >>> cos_udf = UserDefinedFunction.create(
        ...     name="cos",
        ...     sql_function_name="cos",
        ...     function_parameters=[fb.FunctionParameter(name="x", dtype=fb.enum.DBVarType.FLOAT)],
        ...     output_dtype=fb.enum.DBVarType.FLOAT,
        ...     is_global=False,
        ... )

        Use the user-defined function to transform a column in a view:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["cos(Amount)"] = cos_udf(view["Amount"])
        >>> view[["Amount", "cos(Amount)"]].preview(3)
                             GroceryInvoiceGuid           Timestamp  Amount  cos(Amount)
        0  4fccfb1d-02b3-4047-87ab-4e5f910ccdd1 2022-01-03 12:28:58   10.68    -0.310362
        1  9cf3c416-7b38-401e-adf6-1bd26650d1d6 2022-01-03 16:32:15   38.04     0.942458
        2  0a5b99b2-9ff1-452a-a06e-669e8ed4a9fa 2022-01-07 16:20:04    1.99    -0.407033

        Use the same user-defined function to transform a feature:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> cos_feat = cos_udf(feature)
        >>> cos_feat.name = "cos(InvoiceCount_60days)"
        >>> fb.FeatureGroup([feature, cos_feat]).preview(
        ...     observation_set=pd.DataFrame({
        ...         "POINT_IN_TIME": ["2022-06-01 00:00:00"],
        ...         "GROCERYCUSTOMERGUID": ["a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3"],
        ...     })
        ... )
          POINT_IN_TIME                   GROCERYCUSTOMERGUID  InvoiceCount_60days  cos(InvoiceCount_60days)
        0    2022-06-01  a2828c3b-036c-4e2e-9bd6-30c9ee9a20e3                 10.0                 -0.839072


        The user-defined function can also be accessed via the `UDF` handle (the name of the `UserDefinedFunction`
        object is used to access the corresponding function):

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> another_cos_feat = fb.UDF.cos(feature)
        """
        user_defined_function = UserDefinedFunction(
            name=name,
            sql_function_name=sql_function_name,
            function_parameters=function_parameters,
            output_dtype=output_dtype,
            is_global=is_global,
        )
        user_defined_function.save()
        return user_defined_function

    @classmethod
    def get(cls, name: str) -> UserDefinedFunction:
        """
        Get a UserDefinedFunction object by its name.

        Parameters
        ----------
        name: str
            Name of the UserDefinedFunction to retrieve.

        Returns
        -------
        UserDefinedFunction
            UserDefinedFunction object.

        Examples
        --------
        Get a UserDefinedFunction object that is already created.

        >>> udf = fb.UserDefinedFunction.get("cos")  # doctest: +SKIP
        """
        return super().get(name)

    @typechecked
    def update_sql_function_name(self, sql_function_name: str) -> None:
        """
        Update the SQL function name of the user-defined function.

        Parameters
        ----------
        sql_function_name: str
            The SQL function name of the user-defined function (which is used in SQL queries).

        Examples
        --------
        >>> cos_udf = catalog.get_user_defined_function("cos")
        >>> cos_udf.update_sql_function_name("sin")
        >>> cos_udf.sql_function_name
        'sin'
        """
        self.update(
            update_payload={"sql_function_name": sql_function_name}, allow_update_local=False
        )

    @typechecked
    def update_function_parameters(self, function_parameters: List[FunctionParameter]) -> None:
        """
        Update the function parameters of the user-defined function.

        Parameters
        ----------
        function_parameters: List[FunctionParameter]
            The function parameters of the user-defined function.

        Examples
        --------
        >>> cos_udf = catalog.get_user_defined_function("cos")
        >>> cos_udf.update_function_parameters([
        ...     FunctionParameter(name="value", dtype=DBVarType.FLOAT)
        ... ])
        >>> cos_udf.function_parameters
        [FunctionParameter(name='value', dtype='FLOAT', default_value=None, test_value=None)]
        """
        self.update(
            update_payload={"function_parameters": function_parameters}, allow_update_local=False
        )

    @typechecked
    def update_output_dtype(self, output_dtype: Union[DBVarType, str]) -> None:
        """
        Update the output data type of the user-defined function.

        Parameters
        ----------
        output_dtype: Union[DBVarType, str]
            The output data type of the user-defined function.

        Examples
        --------
        >>> cos_udf = catalog.get_user_defined_function("cos")
        >>> cos_udf.update_output_dtype(DBVarType.INT)
        >>> cos_udf.output_dtype
        'INT'
        """
        dtype_value = DBVarType(output_dtype).value
        self.update(update_payload={"output_dtype": dtype_value}, allow_update_local=False)

    def delete(self) -> None:
        """
        Delete the user-defined function from the persistent data store. The user-defined function can only be
        deleted if it is not used by any saved feature.

        Examples
        --------
        Delete a user-defined function:
        >>> udf = catalog.get_user_defined_function("cos")
        >>> udf.delete()
        """
        self._delete()

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary containing the user-defined function's metadata. The dictionary contains the following
        keys:

        - `name`: The name of the user-defined function.
        - `created_at`: The timestamp indicating when the UserDefinedFunction object was created.
        - `updated_at`: The timestamp indicating when the UserDefinedFunction object was last updated.
        - `sql_function_name`: The SQL function name of the user-defined function (which is used in SQL queries).
        - `function_parameters`: The function parameters of the user-defined function.
        - `output_dtype`: The output data type of the user-defined function.
        - `signature`: The signature of the user-defined function.
        - `feature_store_id`: The feature store name that the user-defined function belongs to.
        - `used_by_features`: The list of features that use the user-defined function.

        This method is only available for UserDefinedFunction objects that are saved in the persistent data store.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the user-defined function.

        Examples
        --------
        >>> cos_udf = catalog.get_user_defined_function("cos")
        >>> cos_udf.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    def __call__(self, *args: Any, **kwargs: Any) -> Union[ViewColumn, Feature]:
        udf = cast(UserDefinedFunctionModel, self.cached_model)
        dynamic_func = UserDefinedFunctionInjector.create(udf=udf)
        return dynamic_func(*args, **kwargs)


class UserDefinedFunctionAccessorWrapper:
    """
    UserDefinedFunctionWrapper class used to access user-defined functions dynamically.
    The FunctionAccessor object is constructed dynamically when needed.
    """

    def __dir__(self) -> List[str]:
        # provide auto-completion for user-defined functions by dynamically constructing the function accessor
        # and returning the function accessor's dir list
        attrs = [attr for attr in dir(UserDefinedFunction.func) if not attr.startswith("__")]
        return attrs

    def __getattr__(self, item: str) -> Any:
        # return the attribute of the dynamically constructed function accessor
        return getattr(UserDefinedFunction.func, item)


# create a global instance of the user-defined function accessor handler
UDF = UserDefinedFunctionAccessorWrapper()
