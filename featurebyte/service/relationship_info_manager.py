"""
RelationshipInfoManagerService
"""

from typing import cast

from bson import ObjectId

from featurebyte.exception import DocumentCreationError
from featurebyte.models.entity import ParentEntity
from featurebyte.models.relationship import (
    RelationshipInfoEntityPair,
    RelationshipInfoModel,
    RelationshipInfoServiceUpdate,
    RelationshipStatus,
    RelationshipType,
    RelationTable,
)
from featurebyte.schema.relationship_info import RelationshipInfoCreate, RelationshipInfoUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship import EntityRelationshipService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.relationship_info_validation import RelationshipInfoValidationService
from featurebyte.service.table import TableService


class RelationshipInfoManagerService:
    """
    RelationshipInfoManagerService handles updating relationship info records.

    Notes for relationship type:

    * child_parent: Direct child-parent relationships. ancestor_ids are updated accordingly.
      relationship infos can be used for parent entity lookups.

    * child_parent_shortcut: Fallback child-parent relationships that are not used for parent
      entity lookups, but will be updated to child_parent when other child-parent relationships
      are removed.

    * one_to_one: One-to-one relationships are not used for parent entity lookups. Can only be
      changed by users manually.

    * disabled: Relationship is disabled and not used. Created automatically when there are
      cycles, or can be set manually by users.

    Notes for relationship status:

    * inferred: Relationship info created automatically based on entity tagging.

    * to_review: Relationship info that requires user review. This is set automatically when
      multiple relation table candidates exist between the same entity pair.

    * reviewed: Relationship info that has been reviewed and confirmed by users.

    * conflict: Relationship info that has conflicts (e.g., cycles). Set automatically when
      cycles are detected.
    """

    def __init__(
        self,
        table_service: TableService,
        relationship_info_service: RelationshipInfoService,
        relationship_info_validation_service: RelationshipInfoValidationService,
        entity_service: EntityService,
        entity_relationship_service: EntityRelationshipService,
    ):
        self.table_service = table_service
        self.relationship_info_service = relationship_info_service
        self.relationship_info_validation_service = relationship_info_validation_service
        self.entity_service = entity_service
        self.entity_relationship_service = entity_relationship_service

    async def handle_tagged_entities(self, data_list: list[RelationshipInfoCreate]) -> None:
        """
        Handle requests of adding RelationshipInfo records.

        This is called when entities are tagged to a table. This method will decide whether new
        relationship records will be created, or whether existing relationship info records should
        be updated. ancestor_ids of entities will also be updated accordingly.

        Parameters
        ----------
        data_list : list[RelationshipInfoCreate]
            List of relationship info creation requests

        Raises
        ------
        DocumentCreationError
            If the requested entity tagging would create invalid relationships
        """
        entities_map = {}
        new_data_list = []
        conflict_data_list = []

        for data in data_list:
            if data.entity_id not in entities_map:
                entities_map[data.entity_id] = await self.entity_service.get_document(
                    data.entity_id
                )

            if data.related_entity_id not in entities_map:
                entities_map[data.related_entity_id] = await self.entity_service.get_document(
                    data.related_entity_id
                )

            if data.relationship_type != RelationshipType.CHILD_PARENT:
                # Currently when tagging entities, the relationship type is always CHILD_PARENT.
                # This should be updated if we later support directly creating relationship types
                # other than CHILD_PARENT.
                continue

            child_entity = entities_map[data.entity_id]
            parent_entity = entities_map[data.related_entity_id]

            if parent_entity.id == child_entity.id:
                raise DocumentCreationError(
                    f'Object "{parent_entity.name}" cannot be both parent & child.'
                )

            if child_entity.id in parent_entity.ancestor_ids:
                # In case of cycles, still allow entity tagging but create relationship info records
                # with DISABLED type and CONFLICT status
                conflict_data_list.append(data)
                continue

            has_existing_relationship_info = False
            async for (
                existing_relationship_info
            ) in self.relationship_info_service.list_documents_iterator(
                query_filter={
                    "entity_id": data.entity_id,
                    "related_entity_id": data.related_entity_id,
                },
            ):
                if existing_relationship_info.relationship_status == RelationshipStatus.INFERRED:
                    # A relationship info record between the two entities already exists with status
                    # INFERRED. This means that there is potentially a new relation table candidate
                    # that should be reviewed. No need to create a new record but update the
                    # existing record to TO_REVIEW status.
                    await self.relationship_info_service.update_document(
                        document_id=existing_relationship_info.id,
                        data=RelationshipInfoServiceUpdate(
                            relationship_status=RelationshipStatus.TO_REVIEW
                        ),
                    )
                has_existing_relationship_info = True
                break

            if not has_existing_relationship_info:
                # No existing relationship info found between the two entities. Create a new one if
                # the relationships are verified to be valid.
                new_data_list.append(data)

        # Get existing relationship infos for validation. Retrieve both CHILD_PARENT and
        # CHILD_PARENT_SHORTCUT since the validation might dictate changing their types.
        existing_relationship_infos = {}
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={
                "relationship_type": {
                    "$in": [RelationshipType.CHILD_PARENT, RelationshipType.CHILD_PARENT_SHORTCUT]
                }
            },
        ):
            existing_relationship_infos[relationship_info.id] = relationship_info

        # New relationship infos to be created
        new_relationship_infos = {}
        for data in new_data_list:
            new_relationship_info = RelationshipInfoModel(**data.model_dump(by_alias=True))
            new_relationship_infos[new_relationship_info.id] = new_relationship_info

        # Run validation. If no error, we are ready to create or update relationship infos.
        all_relationship_infos = list(existing_relationship_infos.values()) + list(
            new_relationship_infos.values()
        )
        validation_result = await self.relationship_info_validation_service.validate_relationships(
            all_relationship_infos
        )

        # Unused relationships should be marked as CHILD_PARENT_SHORTCUT. Track their IDs for both
        # existing and new relationship infos
        existing_shortcut_ids = set()
        new_shortcut_ids = set()
        for relationship_info_id in validation_result.unused_relationship_info_ids:
            if relationship_info_id in existing_relationship_infos:
                existing_shortcut_ids.add(relationship_info_id)
            else:
                new_shortcut_ids.add(relationship_info_id)

        # Create new relationship infos records
        for new_data in new_data_list:
            if new_data.id in new_shortcut_ids:
                new_data = new_data.model_copy()
                new_data.relationship_type = RelationshipType.CHILD_PARENT_SHORTCUT
            else:
                # Non-shortcut child parent relationship needs updating entity's ancestor_ids
                table_model = await self.table_service.get_document(
                    document_id=new_data.relation_table_id
                )
                await self.entity_relationship_service.add_relationship(
                    parent=ParentEntity(
                        id=new_data.related_entity_id,
                        table_id=new_data.relation_table_id,
                        table_type=table_model.type,
                    ),
                    child_id=new_data.entity_id,
                    skip_validation=True,
                )
            await self.relationship_info_service.create_document(new_data)

        # Update existing relationship infos to CHILD_PARENT_SHORTCUT if any. No need to update
        # ancestor_ids since they are still valid through non-shortcut relationships
        for relationship_info_id in existing_shortcut_ids:
            await self.relationship_info_service.update_document(
                document_id=relationship_info_id,
                data=RelationshipInfoServiceUpdate(
                    relationship_type=RelationshipType.CHILD_PARENT_SHORTCUT
                ),
            )

        # Create relationship infos with CONFLICT status
        for conflict_data in conflict_data_list:
            conflict_data = conflict_data.model_copy()
            conflict_data.relationship_type = RelationshipType.DISABLED
            relationship_info = await self.relationship_info_service.create_document(conflict_data)
            await self.relationship_info_service.update_document(
                document_id=relationship_info.id,
                data=RelationshipInfoServiceUpdate(relationship_status=RelationshipStatus.CONFLICT),
            )

    async def handle_untagged_entities(
        self,
        pairs: list[RelationshipInfoEntityPair],
    ) -> None:
        """
        Handle requests of removing RelationshipInfo records.

        This is called when entities are untagged from a table. This method will remove existing
        relationship info records between the given entity pairs if there are no remaining relation
        table candidates.

        Relationship info records with CHILD_PARENT_SHORTCUT type might be updated to CHILD_PARENT
        type if the corresponding non-shortcut relationships are removed.

        Parameters
        ----------
        pairs : list[RelationshipInfoEntityPair]
            List of entity pairs for which to remove relationship info records
        """
        relationship_info_ids_to_delete = {}
        relationship_info_ids_to_new_relation_table = {}
        remaining_relationship_infos = {}

        # Get the action to perform for untagged entity pairs (delete vs update to new relation
        # table)
        async for (
            existing_relationship_info
        ) in self.relationship_info_service.list_documents_iterator(
            query_filter={},
        ):
            for pair in pairs:
                entity_id_matched = pair.entity_id is None or (
                    existing_relationship_info.entity_id == pair.entity_id
                )
                related_entity_id_matched = pair.related_entity_id is None or (
                    existing_relationship_info.related_entity_id == pair.related_entity_id
                )
                if entity_id_matched and related_entity_id_matched:
                    relation_tables = await self.get_relation_tables(
                        entity_id=existing_relationship_info.entity_id,
                        related_entity_id=existing_relationship_info.related_entity_id,
                    )
                    if not relation_tables:
                        relationship_info_ids_to_delete[existing_relationship_info.id] = (
                            existing_relationship_info
                        )
                    else:
                        # Update relationship to use another relationship table if the relationship were
                        # to remain, but the current candidate is removed
                        if existing_relationship_info.relation_table_id not in [
                            rel_table.relation_table_id for rel_table in relation_tables
                        ]:
                            relationship_info_ids_to_new_relation_table[
                                existing_relationship_info.id
                            ] = relation_tables[0]
                        remaining_relationship_infos[existing_relationship_info.id] = (
                            existing_relationship_info
                        )
                    break
            else:
                # Doesn't match any untagged pairs, keep as is
                remaining_relationship_infos[existing_relationship_info.id] = (
                    existing_relationship_info
                )

        # Handle parent_child and parent_child_shortcut updates
        await self._validate_relationships_and_handle_child_parent_shortcut(
            remaining_relationship_infos
        )

        # Update relation table for the existing relationship info if applicable
        for rel_info_id, new_relation_table in relationship_info_ids_to_new_relation_table.items():
            existing_relationship_info = remaining_relationship_infos[rel_info_id]
            await self.relationship_info_service.update_document(
                document_id=existing_relationship_info.id,
                data=RelationshipInfoServiceUpdate(
                    relation_table_id=new_relation_table.relation_table_id,
                    entity_column_name=new_relation_table.entity_column_name,
                    related_entity_column_name=new_relation_table.related_entity_column_name,
                    relationship_status=RelationshipStatus.TO_REVIEW,
                ),
            )

        # Delete relationship infos that have no remaining relation table candidates
        for relationship_info_id in relationship_info_ids_to_delete:
            # Delete relationship info record
            relationship_info = relationship_info_ids_to_delete[relationship_info_id]
            await self.relationship_info_service.delete_document(relationship_info_id)

            # Update ancestor_ids if applicable
            child_entity = await self.entity_service.get_document(relationship_info.entity_id)
            has_relationship = [
                par for par in child_entity.parents if par.id == relationship_info.related_entity_id
            ]
            if has_relationship:
                await self.entity_relationship_service.remove_relationship(
                    parent_id=relationship_info.related_entity_id,
                    child_id=relationship_info.entity_id,
                )

    async def get_relation_tables(
        self, entity_id: ObjectId, related_entity_id: ObjectId
    ) -> list[RelationTable]:
        """
        Get the list of available relation tables for a given entity pair.

        Parameters
        ----------
        entity_id: ObjectId
            Entity ID
        related_entity_id: ObjectId
            Related Entity ID

        Returns
        -------
        list[RelationTable]
            List of relation tables
        """
        relation_tables = []

        async for table_model in self.table_service.list_documents_iterator(
            query_filter={"columns_info.entity_id": {"$in": [entity_id, related_entity_id]}}
        ):
            if not table_model.primary_key_columns:
                continue

            primary_key_column = table_model.primary_key_columns[0]
            primary_key_column_info = None
            related_key_column_info = None
            for col_info in table_model.columns_info:
                if col_info.name == primary_key_column and col_info.entity_id == entity_id:
                    primary_key_column_info = col_info
                elif col_info.entity_id == related_entity_id:
                    related_key_column_info = col_info

            # A table is a relation table candidate if its primary key column is tagged to entity_id
            # and another column is tagged to related_entity_id.
            if primary_key_column_info is not None and related_key_column_info is not None:
                relation_tables.append(
                    RelationTable(
                        relation_table_id=table_model.id,
                        entity_column_name=primary_key_column_info.name,
                        related_entity_column_name=related_key_column_info.name,
                    )
                )

        return relation_tables

    async def _validate_relationships_and_handle_child_parent_shortcut(
        self, all_relationship_infos: dict[ObjectId, RelationshipInfoModel]
    ) -> set[ObjectId]:
        validation_result = await self.relationship_info_validation_service.validate_relationships([
            rel_info
            for rel_info in all_relationship_infos.values()
            if rel_info.relationship_type
            in [RelationshipType.CHILD_PARENT, RelationshipType.CHILD_PARENT_SHORTCUT]
        ])

        # Handle shortcut and non-shortcut relationship type updates. No change in ancestor_ids
        # required.
        updated_relationship_info_ids = set()
        for rel_info in all_relationship_infos.values():
            if rel_info.id in validation_result.unused_relationship_info_ids:
                # Mark unused relationships as CHILD_PARENT_SHORTCUT
                if rel_info.relationship_type != RelationshipType.CHILD_PARENT_SHORTCUT:
                    await self.relationship_info_service.update_document(
                        document_id=rel_info.id,
                        data=RelationshipInfoServiceUpdate(
                            relationship_type=RelationshipType.CHILD_PARENT_SHORTCUT
                        ),
                    )
                    updated_relationship_info_ids.add(rel_info.id)
            elif rel_info.relationship_type == RelationshipType.CHILD_PARENT_SHORTCUT:
                # No longer a shortcut relationship based on the relationship infos after untagging.
                # Upgrade to CHILD_PARENT.
                await self.relationship_info_service.update_document(
                    document_id=rel_info.id,
                    data=RelationshipInfoServiceUpdate(
                        relationship_type=RelationshipType.CHILD_PARENT
                    ),
                )
                updated_relationship_info_ids.add(rel_info.id)

        return updated_relationship_info_ids

    async def change_relationship_type(
        self,
        relationship_info: RelationshipInfoModel,
        new_relationship_type: RelationshipType,
    ) -> RelationshipInfoModel:
        """
        Change relationship type of a RelationshipInfoModel

        Parameters
        ----------
        relationship_info: RelationshipInfoModel
            Relationship info model
        new_relationship_type: RelationshipType
            The new relationship type to set

        Returns
        -------
        RelationshipInfoModel
            Updated RelationshipInfo object
        """
        if new_relationship_type == RelationshipType.CHILD_PARENT_SHORTCUT:
            # CHILD_PARENT_SHORTCUT can only be marked internally
            new_relationship_type = RelationshipType.CHILD_PARENT

        # Check which other relationship infos need to be updated as well
        all_relationship_infos = {}
        async for rel_info in self.relationship_info_service.list_documents_iterator(
            query_filter={},
        ):
            # Update the relationship type for the given relationship info since it's not yet
            # updated in persistent yet
            if rel_info.id == relationship_info.id:
                rel_info.relationship_type = new_relationship_type
            all_relationship_infos[rel_info.id] = rel_info

        # Handle parent_child and parent_child_shortcut updates
        updated_relationship_info_ids = (
            await self._validate_relationships_and_handle_child_parent_shortcut(
                all_relationship_infos
            )
        )

        # Update ancestor_ids
        previous_is_child_parent = (
            relationship_info.relationship_type == RelationshipType.CHILD_PARENT
        )
        after_is_child_parent = new_relationship_type == RelationshipType.CHILD_PARENT

        if previous_is_child_parent and not after_is_child_parent:
            # Changing from CHILD_PARENT to non-CHILD_PARENT
            await self.entity_relationship_service.remove_relationship(
                parent_id=relationship_info.related_entity_id,
                child_id=relationship_info.entity_id,
            )
        elif not previous_is_child_parent and after_is_child_parent:
            # Changing from non-CHILD_PARENT to CHILD_PARENT
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
                skip_validation=True,
            )

        # Update relationship type for the given relationship info if it's not already updated above
        if relationship_info.id not in updated_relationship_info_ids:
            await self.relationship_info_service.update_document(
                document_id=relationship_info.id,
                data=RelationshipInfoServiceUpdate(relationship_type=new_relationship_type),
            )

        updated_relationship_info = await self.relationship_info_service.get_document(
            document_id=relationship_info.id
        )
        return updated_relationship_info

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
            await self.change_relationship_type(
                relationship_info=relationship_info,
                new_relationship_type=data.relationship_type,
            )
            # Reset field to None to avoid updating again below
            data.relationship_type = None

        update_data = RelationshipInfoServiceUpdate(**data.model_dump(exclude_unset=True))
        updated_relationship_info = await self.relationship_info_service.update_document(
            document_id=relationship_info_id, data=update_data
        )

        return cast(RelationshipInfoModel, updated_relationship_info)
