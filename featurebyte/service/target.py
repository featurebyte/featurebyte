"""
Target class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.target import TargetModel
from featurebyte.persistent import Persistent
from featurebyte.schema.target import TargetCreate, TargetInfo, TargetServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService


class TargetService(BaseDocumentService[TargetModel, TargetCreate, TargetServiceUpdate]):
    """
    TargetService class
    """

    document_class = TargetModel

    def __init__(
        self, user: Any, persistent: Persistent, catalog_id: ObjectId, entity_service: EntityService
    ):
        super().__init__(user, persistent, catalog_id)
        self.entity_service = entity_service

    async def get_target_info(self, document_id: ObjectId, verbose: bool) -> TargetInfo:
        """
        Get target info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        TargetInfo
        """
        _ = verbose
        target_doc = await self.get_document(document_id=document_id)
        entity_brief_info_list = await self.entity_service.get_entity_brief_info_list(
            set(target_doc.entity_ids)
        )
        return TargetInfo(
            id=document_id,
            target_name=target_doc.name,
            entities=entity_brief_info_list,
            horizon=target_doc.horizon,
            blind_spot=target_doc.blind_spot,
            has_recipe=bool(target_doc.graph),
            created_at=target_doc.created_at,
            updated_at=target_doc.updated_at,
        )
