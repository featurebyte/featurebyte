"""
Relationship Info Service
"""
from typing import Any

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel
from featurebyte.persistent import Persistent
from featurebyte.schema.relationship_info import (
    RelationshipInfoCreate,
    RelationshipInfoInfo,
    RelationshipInfoUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.table import TableService
from featurebyte.service.user_service import UserService


class RelationshipInfoService(
    BaseDocumentService[RelationshipInfoModel, RelationshipInfoCreate, RelationshipInfoUpdate]
):
    """
    RelationshipInfoService class is responsible for keeping track of the relationship info of various types.
    """

    document_class = RelationshipInfoModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        entity_service: EntityService,
        table_service: TableService,
        user_service: UserService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.entity_service = entity_service
        self.table_service = table_service
        self.user_service = user_service

    async def remove_relationship(
        self,
        primary_entity_id: PydanticObjectId,
        related_entity_id: PydanticObjectId,
    ) -> None:
        """
        Remove relationship between primary and related entity

        Parameters
        ----------
        primary_entity_id : PydanticObjectId
            Primary entity id
        related_entity_id : PydanticObjectId
            Related entity id

        Raises
        ------
        DocumentNotFoundError
            If relationship not found
        """
        result = await self.list_documents(
            query_filter={
                "entity_id": primary_entity_id,
                "related_entity_id": related_entity_id,
            },
        )
        data = result["data"]
        if not data:
            # Note that throwing an error here means that this function is not idempotent - meaning that repeated calls
            # to this function will return different results, as one call might succeed, and others would fail.
            # However, given that this function is only called from the user-initiated API, this is probably fine to
            # do for now to raise a more informative user error.
            # Alternatively, we could also make the caller's idempotent by catching this error and ignoring it there
            # in the future.
            raise DocumentNotFoundError(
                f"Relationship not found for primary entity {primary_entity_id} "
                f"and related entity {related_entity_id}."
            )
        assert len(data) == 1
        await self.delete_document(document_id=data[0]["_id"])

    async def get_relationship_info_info(self, document_id: ObjectId) -> RelationshipInfoInfo:
        """
        Get relationship info info

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        RelationshipInfoInfo
        """
        relationship_info = await self.get_document(document_id=document_id)
        table_info = await self.table_service.get_document(
            document_id=relationship_info.relation_table_id
        )
        updated_user_name = self.user_service.get_user_name_for_id(relationship_info.updated_by)
        entity = await self.entity_service.get_document(document_id=relationship_info.entity_id)
        related_entity = await self.entity_service.get_document(
            document_id=relationship_info.related_entity_id
        )
        return RelationshipInfoInfo(
            id=relationship_info.id,
            name=relationship_info.name,
            created_at=relationship_info.created_at,
            updated_at=relationship_info.updated_at,
            relationship_type=relationship_info.relationship_type,
            table_name=table_info.name,
            data_type=table_info.type,
            entity_name=entity.name,
            related_entity_name=related_entity.name,
            updated_by=updated_user_name,
        )
