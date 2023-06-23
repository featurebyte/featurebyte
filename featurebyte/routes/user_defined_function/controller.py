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

    async def create_user_defined_function(
        self,
        data: UserDefinedFunctionCreate,
    ) -> UserDefinedFunctionModel:
        """
        Create UserDefinedFunction at persistent

        Parameters
        ----------
        data: UserDefinedFunctionCreate
            UserDefinedFunction creation payload

        Returns
        -------
        UserDefinedFunctionModel
            Newly created user_defined_function object
        """
        return await self.service.create_document(data)

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
