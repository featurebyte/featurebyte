"""
TableColumnsInfoService
"""

from __future__ import annotations

from typing import List, Tuple, Union

from collections import defaultdict

from bson.objectid import ObjectId
from pymongo.errors import OperationFailure
from tenacity import retry, retry_if_exception_type, wait_chain, wait_random

from featurebyte.enum import TableDataType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import PydanticObjectId, User
from featurebyte.models.entity import ParentEntity
from featurebyte.models.feature_store import TableModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.schema.entity import EntityServiceUpdate
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.relationship import EntityRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService

TableDocumentService = Union[
    EventTableService, ItemTableService, DimensionTableService, SCDTableService
]


class TableColumnsInfoService(OpsServiceMixin):
    """
    TableColumnsInfoService is responsible to orchestrate the update of the table columns info.
    """

    def __init__(
        self,
        user: User,
        persistent: Persistent,
        semantic_service: SemanticService,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        entity_relationship_service: EntityRelationshipService,
    ):
        self.user = user
        self.persistent = persistent
        self.semantic_service = semantic_service
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.entity_relationship_service = entity_relationship_service

    @staticmethod
    async def _validate_column_info_id_field_values(
        columns_info: List[ColumnInfo],
        field_name: str,
        service: Union[EntityService, SemanticService],
        field_class_name: str,
    ) -> None:
        id_values = [
            getattr(col_info, field_name)
            for col_info in columns_info
            if getattr(col_info, field_name)
        ]
        found_id_values = [
            doc.id
            async for doc in service.list_documents_iterator(
                query_filter={"_id": {"$in": id_values}}
            )
        ]
        missing_id_values = sorted(set(id_values).difference(found_id_values))
        if missing_id_values:
            column_name_id_pairs = sorted(
                [
                    (col_info.name, getattr(col_info, field_name))
                    for col_info in columns_info
                    if getattr(col_info, field_name) in missing_id_values
                ]
            )
            col_names, id_vals = zip(*column_name_id_pairs)
            id_vals = [str(id_val) for id_val in id_vals]
            raise DocumentUpdateError(
                f"{field_class_name} IDs {list(id_vals)} not found for columns {list(col_names)}."
            )

    @retry(
        retry=retry_if_exception_type(OperationFailure),
        wait=wait_chain(*[wait_random(max=2) for _ in range(3)]),
    )
    async def update_columns_info(
        self,
        service: TableDocumentService,
        document_id: ObjectId,
        columns_info: List[ColumnInfo],
        skip_block_modification_check: bool = False,
    ) -> None:
        """
        Update table columns info

        Parameters
        ----------
        service: TableDocumentService
            Table service object
        document_id: ObjectId
            Document ID
        columns_info: List[ColumnInfo]
            Columns info
        skip_block_modification_check: bool
            Flag to skip block modification check (used only when updating table column description)
        """
        document = await service.get_document(document_id=document_id)

        if columns_info != document.columns_info:
            # check existence of the entities & semantics
            await self._validate_column_info_id_field_values(
                columns_info=columns_info,
                field_name="entity_id",
                service=self.entity_service,
                field_class_name="Entity",
            )
            await self._validate_column_info_id_field_values(
                columns_info=columns_info,
                field_name="semantic_id",
                service=self.semantic_service,
                field_class_name="Semantic",
            )

            async with self.persistent.start_transaction():
                # update columns info
                await service.update_document(
                    document_id=document_id,
                    data=service.document_update_class(columns_info=columns_info),  # type: ignore
                    skip_block_modification_check=skip_block_modification_check,
                )

                # update entity table reference
                await self.update_entity_table_references(document, columns_info)

    async def _update_entity_table_reference(
        self,
        document: TableModel,
        entity_update: dict[ObjectId, int],
        primary_entity_update: dict[ObjectId, int],
    ) -> None:
        for entity_id, update_flag in entity_update.items():
            primary_update_flag = primary_entity_update.get(entity_id, 0)
            if update_flag == 0 and primary_update_flag == 0:
                continue

            # table reference update is needed
            entity = await self.entity_service.get_document(entity_id)
            update_values = {}
            if update_flag != 0:
                # table references updated in entity
                update_action = (
                    self.include_object_id if update_flag > 0 else self.exclude_object_id
                )
                update_values["table_ids"] = update_action(
                    document_ids=entity.table_ids, document_id=document.id
                )

            if primary_update_flag != 0:
                # primary table references updated in entity
                update_action = (
                    self.include_object_id if primary_update_flag > 0 else self.exclude_object_id
                )
                update_values["primary_table_ids"] = update_action(
                    document_ids=entity.primary_table_ids, document_id=document.id
                )

            await self.entity_service.update_document(
                document_id=entity_id,
                data=EntityServiceUpdate(**update_values),
            )

    @staticmethod
    def _include_parent_entities(
        parents: List[ParentEntity],
        table_id: ObjectId,
        table_type: TableDataType,
        parent_entity_ids: List[ObjectId],
    ) -> Tuple[List[ParentEntity], List[ObjectId]]:
        current_parent_entity_ids = {
            parent.id
            for parent in parents
            if parent.table_id == table_id and parent.table_type == table_type
        }
        additional_parent_entity_ids = sorted(
            set(parent_entity_ids).difference(current_parent_entity_ids)
        )
        output = parents.copy()
        for entity_id in additional_parent_entity_ids:
            output.append(ParentEntity(id=entity_id, table_type=table_type, table_id=table_id))
        return output, additional_parent_entity_ids

    @staticmethod
    def _exclude_parent_entities(
        parents: List[ParentEntity],
        table_id: ObjectId,
        table_type: TableDataType,
        parent_entity_ids: List[ObjectId],
    ) -> Tuple[List[ParentEntity], List[PydanticObjectId]]:
        removed_parent_entity_ids = []
        output = []
        for parent in parents:
            if (
                parent.id in parent_entity_ids
                and parent.table_id == table_id
                and parent.table_type == table_type
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

    @staticmethod
    def _get_column_name_for_entity_id(columns_info: List[ColumnInfo], entity_id: ObjectId) -> str:
        for column_info in columns_info:
            if column_info.entity_id == entity_id:
                return column_info.name
        raise AssertionError(f"Entity column for entity {entity_id} not found")

    async def _add_new_child_parent_relationships(
        self,
        entity_id: ObjectId,
        table_id: ObjectId,
        updated_columns_info: List[ColumnInfo],
        parent_entity_ids_to_add: List[ObjectId],
    ) -> None:
        # Add relationship info links for new parent entity relationships
        entity_column_name = self._get_column_name_for_entity_id(updated_columns_info, entity_id)
        for new_parent_entity_id in parent_entity_ids_to_add:
            related_entity_column_name = self._get_column_name_for_entity_id(
                updated_columns_info, new_parent_entity_id
            )
            await self.relationship_info_service.create_document(
                data=RelationshipInfoCreate(
                    name=f"{entity_id}_{new_parent_entity_id}",
                    relationship_type=RelationshipType.CHILD_PARENT,
                    entity_id=PydanticObjectId(entity_id),
                    related_entity_id=PydanticObjectId(new_parent_entity_id),
                    relation_table_id=PydanticObjectId(table_id),
                    entity_column_name=entity_column_name,
                    related_entity_column_name=related_entity_column_name,
                    enabled=True,
                    updated_by=self.user.id,
                )
            )

    async def _update_entity_relationship(
        self,
        document: TableModel,
        updated_columns_info: List[ColumnInfo],
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
                _, new_parent_entity_ids = self._include_parent_entities(
                    parents=primary_entity.parents,
                    table_id=document.id,
                    table_type=document.type,
                    parent_entity_ids=list(new_parent_entities),
                )
                for parent_id in new_parent_entity_ids:
                    await self.entity_relationship_service.add_relationship(
                        parent=ParentEntity(
                            id=parent_id, table_id=document.id, table_type=document.type
                        ),
                        child_id=entity_id,
                    )

                # Add relationship info links for new parent entity relationships
                await self._add_new_child_parent_relationships(
                    entity_id, document.id, updated_columns_info, new_parent_entity_ids
                )

        if old_diff_new_primary_entities:
            # old primary entities are removed
            for entity_id in old_diff_new_primary_entities:
                primary_entity = await self.entity_service.get_document(document_id=entity_id)
                _, removed_parent_entity_ids = self._exclude_parent_entities(
                    parents=primary_entity.parents,
                    table_id=document.id,
                    table_type=document.type,
                    parent_entity_ids=list(old_parent_entities),
                )
                for parent_id in removed_parent_entity_ids:
                    await self.entity_relationship_service.remove_relationship(
                        parent_id=parent_id,
                        child_id=entity_id,
                    )
                # Remove relationship info links for old parent entity relationships
                await self._remove_parent_entity_ids(entity_id, removed_parent_entity_ids)

        if common_primary_entities:
            # change of non-primary entities
            for entity_id in common_primary_entities:
                primary_entity = await self.entity_service.get_document(document_id=entity_id)
                parents, removed_parent_entity_ids = self._exclude_parent_entities(
                    parents=primary_entity.parents,
                    table_id=document.id,
                    table_type=document.type,
                    parent_entity_ids=list(old_parent_entities.difference(new_parent_entities)),
                )
                parents, new_parent_entity_ids = self._include_parent_entities(
                    parents=parents,
                    table_id=document.id,
                    table_type=document.type,
                    parent_entity_ids=list(new_parent_entities.difference(old_parent_entities)),
                )
                if parents != primary_entity.parents:
                    for parent_id in new_parent_entity_ids:
                        await self.entity_relationship_service.add_relationship(
                            parent=ParentEntity(
                                id=parent_id, table_id=document.id, table_type=document.type
                            ),
                            child_id=entity_id,
                        )
                    for parent_id in removed_parent_entity_ids:
                        await self.entity_relationship_service.remove_relationship(
                            parent_id=parent_id,
                            child_id=entity_id,
                        )
                    # Add relationship info links for new parent entity relationships
                    await self._add_new_child_parent_relationships(
                        entity_id, document.id, updated_columns_info, new_parent_entity_ids
                    )
                    # Remove relationship info links for old parent entity relationships
                    await self._remove_parent_entity_ids(entity_id, removed_parent_entity_ids)

    async def update_entity_table_references(
        self, document: TableModel, columns_info: List[ColumnInfo]
    ) -> None:
        """
        This method prepares table to:
        - update table references in affected entities
        - update entity relationship

        Parameters
        ----------
        document: TableModel
            TableModel object
        columns_info: List[ColumnInfo]
            Columns info
        """
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

        for column_info in columns_info:
            if column_info.entity_id:
                # flag for addition to entity & track new entity relationship
                entity_update[column_info.entity_id] += 1
                if column_info.name in primary_keys:
                    primary_entity_update[column_info.entity_id] += 1
                    new_primary_entities.add(column_info.entity_id)
                else:
                    new_parent_entities.add(column_info.entity_id)

        # actual entity document update
        await self._update_entity_table_reference(
            document=document,
            entity_update=entity_update,
            primary_entity_update=primary_entity_update,
        )
        await self._update_entity_relationship(
            document=document,
            updated_columns_info=columns_info,
            old_primary_entities=old_primary_entities,
            old_parent_entities=old_parent_entities,
            new_primary_entities=new_primary_entities,
            new_parent_entities=new_parent_entities,
        )
