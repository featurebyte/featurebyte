"""
UserDefinedFunctionService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.exception import DocumentConflictError
from featurebyte.models.persistent import QueryFilter
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.schema.user_defined_function import (
    UserDefinedFunctionServiceCreate,
    UserDefinedFunctionServiceUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class UserDefinedFunctionService(
    BaseDocumentService[
        UserDefinedFunctionModel, UserDefinedFunctionServiceCreate, UserDefinedFunctionServiceUpdate
    ]
):
    """
    UserDefinedFunctionService class
    """

    document_class = UserDefinedFunctionModel

    async def construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        output = await super().construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # user defined function without catalog_id is a global function (used by all catalogs)
        output["catalog_id"] = {"$in": [None, self.catalog_id]}
        return output

    async def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        output = await super().construct_list_query_filter(
            query_filter=query_filter, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # user defined function without catalog_id is a global function (used by all catalogs)
        output["catalog_id"] = {"$in": [None, self.catalog_id]}
        return output

    async def create_document(
        self, data: UserDefinedFunctionServiceCreate
    ) -> UserDefinedFunctionModel:
        # check if user defined function with same name already exists
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": data.name, "catalog_id": data.catalog_id},
        )
        if document_dict:
            if data.catalog_id:
                raise DocumentConflictError(
                    f'User defined function with name "{data.name}" already exists in '
                    f"catalog (catalog_id: {data.catalog_id})."
                )

            raise DocumentConflictError(
                f'Global user defined function with name "{data.name}" already exists.'
            )
        return await super().create_document(data=data)
