"""
UserDefinedFunction API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, cast

from collections import OrderedDict

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
    UserDefinedFunctionServiceUpdate,
    UserDefinedFunctionUpdate,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.user_defined_function import UserDefinedFunctionService


class UserDefinedFunctionController(
    BaseDocumentController[
        UserDefinedFunctionModel, UserDefinedFunctionService, UserDefinedFunctionList
    ]
):
    """
    UserDefinedFunctionController class
    """

    paginated_document_class = UserDefinedFunctionList

    def __init__(
        self,
        user_defined_function_service: UserDefinedFunctionService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(service=user_defined_function_service)
        self.feature_service = feature_service
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def create_user_defined_function(
        self,
        data: UserDefinedFunctionCreate,
        get_credential: Any,
    ) -> UserDefinedFunctionModel:
        """
        Create UserDefinedFunction at persistent

        Parameters
        ----------
        data: UserDefinedFunctionCreate
            UserDefinedFunction creation payload
        get_credential: Any
            Function to get credential

        Returns
        -------
        UserDefinedFunctionModel
            Newly created user_defined_function object
        """
        # validate feature store id exists
        feature_store = await self.feature_store_service.get_document(
            document_id=data.feature_store_id
        )

        # create user defined function
        user_defined_function = await self.service.create_document(data)
        try:
            # check if user defined function exists in warehouse
            await self.feature_store_warehouse_service.check_user_defined_function_exists(
                user_defined_function=user_defined_function,
                feature_store=feature_store,
                get_credential=get_credential,
            )
        except Exception as exc:
            # if not exists, delete the user defined function
            await self.service.delete_document(document_id=user_defined_function.id)
            raise DocumentCreationError(f"User defined function not exists: {exc}") from exc

        return user_defined_function

    async def update_user_defined_function(
        self,
        document_id: PydanticObjectId,
        data: UserDefinedFunctionUpdate,
    ) -> UserDefinedFunctionModel:
        """
        Update UserDefinedFunction at persistent

        Parameters
        ----------
        document_id: PydanticObjectId
            UserDefinedFunction id
        data: UserDefinedFunctionUpdate
            UserDefinedFunction update payload

        Returns
        -------
        UserDefinedFunctionModel
            Updated user_defined_function object
        """
        # check if user defined function exists
        document = await self.service.get_document(document_id=document_id)

        # check if function used in any saved feature
        features = await self.feature_service.list_documents(
            query_filter={"user_defined_function_ids": {"$in": [document_id]}},
        )
        if features["total"]:
            feature_names = [doc["name"] for doc in features["data"]]
            raise DocumentUpdateError(
                f"User defined function used by saved feature(s): {feature_names}"
            )

        func_param_mapping = OrderedDict()
        for func_param in document.function_parameters:
            func_param_mapping[func_param.name] = func_param

        for func_param in data.function_parameters:
            if func_param.name in func_param_mapping:
                func_param_mapping[func_param.name] = func_param
            else:
                raise DocumentUpdateError(f"Function parameter not exists: {func_param.name}")

        function_parameters = list(func_param_mapping.values())
        if function_parameters == document.function_parameters:
            raise DocumentUpdateError("No changes found in function parameters")

        updated_document = await self.service.update_document(
            document_id=document_id,
            data=UserDefinedFunctionServiceUpdate(function_parameters=function_parameters),
        )
        return cast(UserDefinedFunctionModel, updated_document)

    async def list_user_defined_functions(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        name: str | None = None,
        feature_store_id: PydanticObjectId | None = None,
    ) -> UserDefinedFunctionList:
        """
        List UserDefinedFunction stored in persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        feature_store_id: PydanticObjectId | None
            FeatureStore id to be used in filtering

        Returns
        -------
        UserDefinedFunctionList
            Paginated list of UserDefinedFunction
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        if feature_store_id:
            params["query_filter"] = {"feature_store_id": feature_store_id}
        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **params,
        )
