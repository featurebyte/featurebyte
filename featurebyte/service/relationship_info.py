"""
Relationship Info Service
"""
from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfo
from featurebyte.schema.relationship_info import RelationshipInfoCreate, RelationshipInfoUpdate
from featurebyte.service.base_document import BaseDocumentService


class RelationshipInfoService(
    BaseDocumentService[RelationshipInfo, RelationshipInfoCreate, RelationshipInfoUpdate]
):
    """
    RelationshipInfoService class is responsible for keeping track of the relationship info of various types.
    """

    document_class = RelationshipInfo

    async def remove_relationship(
        self,
        primary_entity_id: PydanticObjectId,
        related_entity_id: PydanticObjectId,
        user_id: ObjectId,
    ) -> None:
        """
        Remove relationship between primary and related entity

        Parameters
        ----------
        primary_entity_id : PydanticObjectId
            Primary entity id
        related_entity_id : PydanticObjectId
            Related entity id
        user_id : ObjectId
            User id
        """
        await self.persistent.delete_one(
            collection_name=self.collection_name,
            query_filter={
                "primary_entity_id": primary_entity_id,
                "related_entity_id": related_entity_id,
            },
            user_id=user_id,
        )
