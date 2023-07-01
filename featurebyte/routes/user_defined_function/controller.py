"""
UserDefinedFunction API route controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionList,
)
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
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(service=user_defined_function_service)
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
        except Exception:
            # if not exists, delete the user defined function
            await self.service.delete_document(document_id=user_defined_function.id)
            raise

        return user_defined_function

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
