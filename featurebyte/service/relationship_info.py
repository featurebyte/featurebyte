"""
Relationship Info Service
"""

from typing import Any, Optional

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoCreate, RelationshipInfoUpdate
from featurebyte.service.base_document import BaseDocumentService


class RelationshipInfoService(
    BaseDocumentService[RelationshipInfoModel, RelationshipInfoCreate, RelationshipInfoUpdate]
):
    """
    RelationshipInfoService class is responsible for keeping track of the relationship info of various types.
    """

    document_class = RelationshipInfoModel

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
        result = await self.list_documents_as_dict(
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

    async def remove_one_to_one_relationships(
        self,
        primary_entity_id: Optional[PydanticObjectId],
        related_entity_id: Optional[PydanticObjectId],
        relation_table_id: PydanticObjectId,
    ) -> None:
        """
        Remove a one-to-one relationship between primary and related entity in a relation table

        Parameters
        ----------
        primary_entity_id : Optional[PydanticObjectId]
            Primary entity id
        related_entity_id: Optional[PydanticObjectId]
            Related entity id
        relation_table_id : PydanticObjectId
            Relation table id
        """
        query_filter: dict[str, Any] = {"relationship_type": RelationshipType.ONE_TO_ONE}
        if primary_entity_id is not None:
            query_filter["entity_id"] = primary_entity_id
        if related_entity_id is not None:
            query_filter["related_entity_id"] = related_entity_id
        query_filter["relation_table_id"] = relation_table_id
        async for relationship_info in self.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"id": 1}
        ):
            await self.delete_document(document_id=relationship_info["_id"])
