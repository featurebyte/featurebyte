"""
DataStatusService
"""
from __future__ import annotations

from typing import Any, List, Tuple, Union

from collections import defaultdict

from bson.objectid import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import ParentEntity
from featurebyte.models.feature_store import DataModel, DataStatus
from featurebyte.models.relationship import RelationshipType
from featurebyte.persistent import Persistent
from featurebyte.schema.dimension_data import DimensionDataServiceUpdate
from featurebyte.schema.entity import EntityServiceUpdate
from featurebyte.schema.event_data import EventDataServiceUpdate
from featurebyte.schema.item_data import ItemDataServiceUpdate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.schema.scd_data import SCDDataServiceUpdate
from featurebyte.service.base_service import BaseService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService

DataDocumentService = Union[EventDataService, ItemDataService, DimensionDataService, SCDDataService]
DataServiceUpdateSchema = Union[
    EventDataServiceUpdate, ItemDataServiceUpdate, DimensionDataServiceUpdate, SCDDataServiceUpdate
]


class DataUpdateService(BaseService):
    """
    DataStatusService is responsible to update the data status.
    """

    def __init__(self, user: Any, persistent: Persistent, workspace_id: ObjectId):
        super().__init__(user, persistent, workspace_id)
        self.feature_store_service = FeatureStoreService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.semantic_service = SemanticService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.entity_service = EntityService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.relationship_info_service = RelationshipInfoService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )

    @staticmethod
    async def update_data_status(
        service: DataDocumentService, document_id: ObjectId, data: DataServiceUpdateSchema
    ) -> None:
        """
        Update data status

        Parameters
        ----------
        service: DataDocumentService
            Data service object
        document_id: ObjectId
            Document ID
        data: DataServiceUpdateSchema
            Data upload payload

        Raises
        ------
        DocumentUpdateError
            When the data status transition is invalid
        """
        document = await service.get_document(document_id=document_id)

        current_status = document.status
        if data.status is not None and current_status != data.status:
            # check eligibility of status transition
            eligible_transitions = {
                DataStatus.DRAFT: {DataStatus.PUBLISHED},
                DataStatus.PUBLISHED: {DataStatus.DEPRECATED},
                DataStatus.DEPRECATED: {},
            }
            if data.status not in eligible_transitions[current_status]:
                raise DocumentUpdateError(
                    f"Invalid status transition from {current_status} to {data.status}."
                )
            await service.update_document(
                document_id=document_id,
                data=type(data)(status=data.status),  # type: ignore
                return_document=False,
            )

    @staticmethod
    async def _validate_column_info_id_field_values(
        data: DataServiceUpdateSchema,
        field_name: str,
        service: Union[EntityService, SemanticService],
        field_class_name: str,
    ) -> None:
        assert data.columns_info is not None
        id_values = [
            getattr(col_info, field_name)
            for col_info in data.columns_info
            if getattr(col_info, field_name)
        ]
        found_id_values = [
            ObjectId(doc["_id"])
            async for doc in service.list_documents_iterator(
                query_filter={"_id": {"$in": id_values}}
            )
        ]
        missing_id_values = sorted(set(id_values).difference(found_id_values))
        if missing_id_values:
            column_name_id_pairs = sorted(
                [
                    (col_info.name, getattr(col_info, field_name))
                    for col_info in data.columns_info
                    if getattr(col_info, field_name) in missing_id_values
                ]
            )
            col_names, id_vals = zip(*column_name_id_pairs)
            id_vals = [str(id_val) for id_val in id_vals]
            raise DocumentUpdateError(
                f"{field_class_name} IDs {list(id_vals)} not found for columns {list(col_names)}."
            )

    async def update_columns_info(
        self, service: DataDocumentService, document_id: ObjectId, data: DataServiceUpdateSchema
    ) -> None:
        """
        Update data columns info

        Parameters
        ----------
        service: DataDocumentService
            Data service object
        document_id: ObjectId
            Document ID
        data: DataServiceUpdateSchema
            Data upload payload
        """
        document = await service.get_document(document_id=document_id)

        if data.columns_info is not None and data.columns_info != document.columns_info:
            # check existence of the entities & semantics
            await self._validate_column_info_id_field_values(
                data=data,
                field_name="entity_id",
                service=self.entity_service,
                field_class_name="Entity",
            )
            await self._validate_column_info_id_field_values(
                data=data,
                field_name="semantic_id",
                service=self.semantic_service,
                field_class_name="Semantic",
            )

            async with self.persistent.start_transaction():

                # update columns info
                await service.update_document(
                    document_id=document_id,
                    data=type(data)(columns_info=data.columns_info),  # type: ignore
                )

                # update entity data reference
                await self.update_entity_data_references(document, data)

    async def _update_entity_tabular_data_reference(
        self,
        document: DataModel,
        entity_update: dict[ObjectId, int],
        primary_entity_update: dict[ObjectId, int],
    ) -> None:
        for entity_id, update_flag in entity_update.items():
            primary_update_flag = primary_entity_update.get(entity_id, 0)
            if update_flag == 0 and primary_update_flag == 0:
                continue

            # data reference update is needed
            entity = await self.entity_service.get_document(entity_id)
            update_values = {}
            if update_flag != 0:
                # data references updated in entity
                update_action = (
                    self.include_object_id if update_flag > 0 else self.exclude_object_id
                )
                update_values["tabular_data_ids"] = update_action(
                    document_ids=entity.tabular_data_ids, document_id=document.id
                )

            if primary_update_flag != 0:
                # primary data references updated in entity
                update_action = (
                    self.include_object_id if primary_update_flag > 0 else self.exclude_object_id
                )
                update_values["primary_tabular_data_ids"] = update_action(
                    document_ids=entity.primary_tabular_data_ids, document_id=document.id
                )

            await self.entity_service.update_document(
                document_id=entity_id,
                data=EntityServiceUpdate(**update_values),
            )

    @staticmethod
    def _include_parent_entities(
        parents: List[ParentEntity],
        data_id: ObjectId,
        data_type: TableDataType,
        parent_entity_ids: List[ObjectId],
    ) -> Tuple[List[ParentEntity], List[ObjectId]]:
        current_parent_entity_ids = {
            parent.id
            for parent in parents
            if parent.data_id == data_id and parent.data_type == data_type
        }
        additional_parent_entity_ids = sorted(
            set(parent_entity_ids).difference(current_parent_entity_ids)
        )
        output = parents.copy()
        for entity_id in additional_parent_entity_ids:
            output.append(ParentEntity(id=entity_id, data_type=data_type, data_id=data_id))
        return output, additional_parent_entity_ids

    @staticmethod
    def _exclude_parent_entities(
        parents: List[ParentEntity],
        data_id: ObjectId,
        data_type: TableDataType,
        parent_entity_ids: List[ObjectId],
    ) -> Tuple[List[ParentEntity], List[PydanticObjectId]]:
        removed_parent_entity_ids = []
        output = []
        for parent in parents:
            if (
                parent.id in parent_entity_ids
                and parent.data_id == data_id
                and parent.data_type == data_type
            ):
                removed_parent_entity_ids.append(parent.id)
                continue
            output.append(parent)
        return output, removed_parent_entity_ids

    async def _remove_parent_entity_ids(
        self, primary_entity_id: ObjectId, parent_entity_ids_to_remove: List[PydanticObjectId]
    ) -> None:
        # Remove relationship info links for old parent entity relationships
        for removed_parent_entity_id in parent_entity_ids_to_remove:
            await self.relationship_info_service.remove_relationship(
                primary_entity_id=PydanticObjectId(primary_entity_id),
                related_entity_id=removed_parent_entity_id,
            )

    async def _add_new_child_parent_relationships(
        self,
        primary_entity_id: ObjectId,
        data_source_id: ObjectId,
        parent_entity_ids_to_add: List[ObjectId],
    ) -> None:
        # Add relationship info links for new parent entity relationships
        for new_parent_entity_id in parent_entity_ids_to_add:
            await self.relationship_info_service.create_document(
                data=RelationshipInfoCreate(
                    name=f"{primary_entity_id}_{new_parent_entity_id}",
                    relationship_type=RelationshipType.CHILD_PARENT,
                    primary_entity_id=PydanticObjectId(primary_entity_id),
                    related_entity_id=PydanticObjectId(new_parent_entity_id),
                    primary_data_source_id=PydanticObjectId(data_source_id),
                    is_enabled=True,
                )
            )

    async def _update_entity_relationship(
        self,
        document: DataModel,
        old_primary_entities: set[ObjectId],
        old_parent_entities: set[ObjectId],
        new_primary_entities: set[ObjectId],
        new_parent_entities: set[ObjectId],
    ) -> None:
        new_diff_old_primary_entities = new_primary_entities.difference(old_primary_entities)
        old_diff_new_primary_entities = old_primary_entities.difference(new_primary_entities)
        common_primary_entities = new_primary_entities.intersection(old_primary_entities)
        if new_diff_old_primary_entities:
            # new primary entities are introduced
            for entity_id in new_diff_old_primary_entities:
                primary_entity = await self.entity_service.get_document(document_id=entity_id)
                parents, new_parent_entity_ids = self._include_parent_entities(
                    parents=primary_entity.parents,
                    data_id=document.id,
                    data_type=document.type,
                    parent_entity_ids=list(new_parent_entities),
                )
                await self.entity_service.update_document(
                    document_id=primary_entity.id, data=EntityServiceUpdate(parents=parents)
                )

                # Add relationship info links for new parent entity relationships
                await self._add_new_child_parent_relationships(
                    entity_id, document.id, new_parent_entity_ids
                )

        if old_diff_new_primary_entities:
            # old primary entities are removed
            for entity_id in old_diff_new_primary_entities:
                primary_entity = await self.entity_service.get_document(document_id=entity_id)
                parents, removed_parent_entity_ids = self._exclude_parent_entities(
                    parents=primary_entity.parents,
                    data_id=document.id,
                    data_type=document.type,
                    parent_entity_ids=list(old_parent_entities),
                )
                await self.entity_service.update_document(
                    document_id=primary_entity.id, data=EntityServiceUpdate(parents=parents)
                )

                # Remove relationship info links for old parent entity relationships
                await self._remove_parent_entity_ids(entity_id, removed_parent_entity_ids)

        if common_primary_entities:
            # change of non-primary entities
            for entity_id in common_primary_entities:
                primary_entity = await self.entity_service.get_document(document_id=entity_id)
                parents, removed_parent_entity_ids = self._exclude_parent_entities(
                    parents=primary_entity.parents,
                    data_id=document.id,
                    data_type=document.type,
                    parent_entity_ids=list(old_parent_entities.difference(new_parent_entities)),
                )
                parents, new_parent_entity_ids = self._include_parent_entities(
                    parents=parents,
                    data_id=document.id,
                    data_type=document.type,
                    parent_entity_ids=list(new_parent_entities.difference(old_parent_entities)),
                )
                if parents != primary_entity.parents:
                    await self.entity_service.update_document(
                        document_id=primary_entity.id, data=EntityServiceUpdate(parents=parents)
                    )
                    # Add relationship info links for new parent entity relationships
                    await self._add_new_child_parent_relationships(
                        entity_id, document.id, new_parent_entity_ids
                    )
                    # Remove relationship info links for old parent entity relationships
                    await self._remove_parent_entity_ids(entity_id, removed_parent_entity_ids)

    async def update_entity_data_references(
        self, document: DataModel, data: DataServiceUpdateSchema
    ) -> None:
        """
        Update data columns info

        Parameters
        ----------
        document: DataModel
            DataModel object
        data: DataServiceUpdateSchema
            Data upload payload
        """
        if not data.columns_info:
            return

        # prepare data to:
        # - update data references in affected entities
        # - update entity relationship
        primary_keys = document.primary_key_columns
        entity_update: dict[ObjectId, int] = defaultdict(int)
        primary_entity_update: dict[ObjectId, int] = defaultdict(int)
        old_primary_entities: set[ObjectId] = set()
        old_parent_entities: set[ObjectId] = set()
        new_primary_entities: set[ObjectId] = set()
        new_parent_entities: set[ObjectId] = set()
        for column_info in document.columns_info:
            if column_info.entity_id:
                # flag for removal from entity & track old entity relationship
                entity_update[column_info.entity_id] -= 1
                if column_info.name in primary_keys:
                    primary_entity_update[column_info.entity_id] -= 1
                    old_primary_entities.add(column_info.entity_id)
                else:
                    old_parent_entities.add(column_info.entity_id)

        for column_info in data.columns_info:
            if column_info.entity_id:
                # flag for addition to entity & track new entity relationship
                entity_update[column_info.entity_id] += 1
                if column_info.name in primary_keys:
                    primary_entity_update[column_info.entity_id] += 1
                    new_primary_entities.add(column_info.entity_id)
                else:
                    new_parent_entities.add(column_info.entity_id)

        # actual entity document update
        await self._update_entity_tabular_data_reference(
            document=document,
            entity_update=entity_update,
            primary_entity_update=primary_entity_update,
        )
        await self._update_entity_relationship(
            document=document,
            old_primary_entities=old_primary_entities,
            old_parent_entities=old_parent_entities,
            new_primary_entities=new_primary_entities,
            new_parent_entities=new_parent_entities,
        )
