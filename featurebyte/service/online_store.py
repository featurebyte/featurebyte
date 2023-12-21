"""
OnlineStoreService class
"""
from __future__ import annotations

from typing import Type

from bson import ObjectId

from featurebyte.models.online_store import OnlineStoreModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.info import OnlineStoreInfo
from featurebyte.schema.online_store import OnlineStoreCreate
from featurebyte.service.base_document import BaseDocumentService


class OnlineStoreService(
    BaseDocumentService[OnlineStoreModel, OnlineStoreCreate, BaseDocumentServiceUpdateSchema],
):
    """
    OnlineStoreService class
    """

    document_class: Type[OnlineStoreModel] = OnlineStoreModel

    async def get_online_store_info(self, document_id: ObjectId, verbose: bool) -> OnlineStoreInfo:
        """
        Get online store info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        OnlineStoreInfo
        """
        _ = verbose
        online_store = await self.get_document(document_id=document_id)
        return OnlineStoreInfo(
            name=online_store.name,
            created_at=online_store.created_at,
            updated_at=online_store.updated_at,
            details=online_store.details,
            description=online_store.description,
        )
