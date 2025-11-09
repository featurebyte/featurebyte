"""
RelationshipInfoUpdateService
"""
from typing import cast

from bson import ObjectId

from featurebyte.models.entity import ParentEntity
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoUpdate
from featurebyte.service.relationship import EntityRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.table import TableService


class RelationshipInfoUpdateService:
    """
    RelationshipInfoUpdateService handles updating relationship info records.
    """

    def __init__(
        self,
        table_service: TableService,
        relationship_info_service: RelationshipInfoService,
        entity_relationship_service: EntityRelationshipService,
    ):
        self.table_service = table_service
        self.relationship_info_service = relationship_info_service
        self.entity_relationship_service = entity_relationship_service

    async def update_relationship_info(
        self,
        relationship_info_id: ObjectId,
        data: RelationshipInfoUpdate,
    ) -> RelationshipInfoModel:
        """
        Update RelationshipInfo

        Parameters
        ----------
        relationship_info_id: ObjectId
            RelationshipInfo ID
        data: RelationshipInfoUpdate
            RelationshipInfo update payload

        Returns
        -------
        RelationshipInfoModel
            Updated RelationshipInfo object
        """
        relationship_info = await self.relationship_info_service.get_document(
            document_id=relationship_info_id
        )

        if (
            data.relationship_type is not None
            and data.relationship_type != relationship_info.relationship_type
        ):
            if data.relationship_type == RelationshipType.ONE_TO_ONE:
                # Changing from CHILD_PARENT to ONE_TO_ONE
                await self.entity_relationship_service.remove_relationship(
                    parent_id=relationship_info.related_entity_id,
                    child_id=relationship_info.entity_id,
                )
            else:
                # Changing from ONE_TO_ONE to CHILD_PARENT
                table_model = await self.table_service.get_document(
                    document_id=relationship_info.relation_table_id
                )
                await self.entity_relationship_service.add_relationship(
                    parent=ParentEntity(
                        id=relationship_info.related_entity_id,
                        table_id=relationship_info.relation_table_id,
                        table_type=table_model.type,
                    ),
                    child_id=relationship_info.entity_id,
                )

        updated_relationship_info = await self.relationship_info_service.update_document(
            document_id=relationship_info_id, data=data
        )
        return cast(RelationshipInfoModel, updated_relationship_info)
