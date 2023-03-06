"""
Relationship Info Service
"""
from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
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
        """
        result = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={
                "primary_entity_id": primary_entity_id,
                "related_entity_id": related_entity_id,
            },
        )
        if not result:
            raise DocumentNotFoundError(
                f"Relationship not found for primary entity {primary_entity_id} "
                f"and related entity {related_entity_id}."
            )
        await self.delete_document(
            document_id=result["_id"],
        )
