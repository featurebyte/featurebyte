"""
UserDefinedFunctionService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.exception import DocumentConflictError
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.persistent import Persistent
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionCreate,
    UserDefinedFunctionUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService


class UserDefinedFunctionService(
    BaseDocumentService[
        UserDefinedFunctionModel, UserDefinedFunctionCreate, UserDefinedFunctionUpdate
    ]
):
    """
    UserDefinedFunctionService class
    """

    document_class = UserDefinedFunctionModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
        )
        self.feature_store_service = feature_store_service

    def _construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        query_filter = super()._construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # user defined function without catalog_id is a global function (used by all catalogs)
        query_filter["catalog_id"] = {"$in": [None, self.catalog_id]}
        return query_filter

    async def create_document(self, data: UserDefinedFunctionCreate) -> UserDefinedFunctionModel:
        # validate feature store id exists first
        _ = await self.feature_store_service.get_document(document_id=data.feature_store_id)

        # check if user defined function with same name already exists
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": data.name, "catalog_id": {"$in": [None, self.catalog_id]}},
            user_id=self.user.id,
        )
        if document_dict:
            if document_dict["catalog_id"] is None:
                raise DocumentConflictError(
                    f'Global user defined function with name "{data.name}" already exists.'
                )
            else:
                raise DocumentConflictError(
                    f'User defined function with name "{data.name}" already exists in '
                    f"catalog (catalog_id: {self.catalog_id})."
                )

        return await super().create_document(data=data)
