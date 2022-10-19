"""
ItemDataService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.feature_store import DataStatus
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService


class ItemDataService(BaseDocumentService[ItemDataModel]):
    """
    ItemDataService class
    """

    document_class = ItemDataModel

    async def create_document(  # type: ignore[override]
        self, data: ItemDataCreate, get_credential: Any = None
    ) -> ItemDataModel:
        _ = get_credential
        _ = await FeatureStoreService(user=self.user, persistent=self.persistent).get_document(
            document_id=data.tabular_source.feature_store_id
        )
        document = ItemDataModel(user_id=self.user.id, status=DataStatus.DRAFT, **data.json_dict())

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: ItemDataUpdate,
        exclude_none: bool = True,
        document: Optional[ItemDataModel] = None,
        return_document: bool = True,
    ) -> Optional[ItemDataModel]:
        if document is None:
            await self.get_document(document_id=document_id)

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": data.dict(exclude_none=exclude_none)},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None
