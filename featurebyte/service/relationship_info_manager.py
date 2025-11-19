"""
RelationshipInfoManagerService
"""

from dataclasses import dataclass, field
from typing import Optional, cast

from bson import ObjectId

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.models.entity import EntityModel, ParentEntity
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


@dataclass
class TaggingRequestClassification:
    """
    Classification of entity tagging requests after initial validation.
    """

    new_relationships: list[RelationshipInfoCreate] = field(default_factory=list)
    conflicts: list[RelationshipInfoCreate] = field(default_factory=list)
    to_review_ids: set[ObjectId] = field(default_factory=set)


@dataclass
class ShortcutDetermination:
    """
    Determination of which relationships should be shortcuts.
    """

    new_shortcuts: set[ObjectId] = field(default_factory=set)
    existing_shortcuts: set[ObjectId] = field(default_factory=set)


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
        None
        """
        # Classify the incoming tagging requests
        entities_map = await self._fetch_entities_for_tagging(data_list)
        classification = await self._classify_tagging_requests(data_list, entities_map)

        # Determine which relationships should be shortcuts
        shortcut_determination = await self._determine_shortcut_relationships(
            classification.new_relationships
        )

        # Execute the classified actions
        await self._create_new_relationships(
            classification.new_relationships, shortcut_determination
        )
        await self._mark_existing_as_shortcuts(shortcut_determination.existing_shortcuts)
        await self._create_conflict_relationships(classification.conflicts)
        await self._mark_relationships_for_review(classification.to_review_ids)

    async def _fetch_entities_for_tagging(
        self, data_list: list[RelationshipInfoCreate]
    ) -> dict[ObjectId, EntityModel]:
        """
        Fetch all entities referenced in the tagging requests.

        Parameters
        ----------
        data_list : list[RelationshipInfoCreate]
            List of relationship info creation requests

        Returns
        -------
        dict[ObjectId, EntityModel]
            Dictionary mapping entity IDs to EntityModel objects
        """
        entity_ids = set()
        for data in data_list:
            entity_ids.add(data.entity_id)
            entity_ids.add(data.related_entity_id)

        entities_map = {}
        for entity_id in entity_ids:
            entities_map[entity_id] = await self.entity_service.get_document(entity_id)

        return entities_map

    async def _classify_tagging_requests(
        self,
        data_list: list[RelationshipInfoCreate],
        entities_map: dict[ObjectId, EntityModel],
    ) -> TaggingRequestClassification:
        """
        Classify tagging requests into new relationships, conflicts, or review updates.

        Parameters
        ----------
        data_list : list[RelationshipInfoCreate]
            List of relationship info creation requests
        entities_map : dict[ObjectId, EntityModel]
            Dictionary mapping entity IDs to EntityModel objects

        Returns
        -------
        TaggingRequestClassification
            Classification containing new relationships to create, conflicts to handle,
            and existing relationship IDs to mark for review.

        Raises
        ------
        DocumentCreationError
            If an entity cannot be both parent and child of itself
        """
        classification = TaggingRequestClassification()

        for data in data_list:
            # Skip non-CHILD_PARENT relationships (not currently supported in tagging)
            if data.relationship_type != RelationshipType.CHILD_PARENT:
                continue

            child_entity = entities_map[data.entity_id]
            parent_entity = entities_map[data.related_entity_id]

            # Validate: entity cannot be parent of itself
            if parent_entity.id == child_entity.id:
                raise DocumentCreationError(
                    f'Object "{parent_entity.name}" cannot be both parent & child.'
                )

            # Detect cycles: would create a cycle if child is already ancestor of parent
            if child_entity.id in parent_entity.ancestor_ids:
                classification.conflicts.append(data)
                continue

            # Check if relationship already exists
            existing_relationship = await self._find_existing_relationship_info(
                entity_id=data.entity_id,
                related_entity_id=data.related_entity_id,
            )

            if existing_relationship:
                # New relation table candidate found: mark for user review
                if existing_relationship.relationship_status == RelationshipStatus.INFERRED:
                    classification.to_review_ids.add(existing_relationship.id)
            else:
                # Completely new relationship
                classification.new_relationships.append(data)

        return classification

    async def _find_existing_relationship_info(
        self,
        entity_id: ObjectId,
        related_entity_id: ObjectId,
    ) -> Optional[RelationshipInfoModel]:
        """
        Find existing relationship info between two entities, if any.

        Parameters
        ----------
        entity_id : ObjectId
            The child entity ID
        related_entity_id : ObjectId
            The parent entity ID

        Returns
        -------
        Optional[RelationshipInfoModel]
            The existing relationship info if found, None otherwise
        """
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={
                "entity_id": entity_id,
                "related_entity_id": related_entity_id,
            },
        ):
            return relationship_info  # Return first match
        return None

    async def _determine_shortcut_relationships(
        self,
        new_relationships: list[RelationshipInfoCreate],
    ) -> ShortcutDetermination:
        """
        Determine which relationships should be shortcuts based on graph validation.

        A relationship is a "shortcut" if it's redundant, i.e., the same entity connection exists
        through a longer path in the relationship graph.

        Parameters
        ----------
        new_relationships : list[RelationshipInfoCreate]
            List of new relationship creation requests to analyze

        Returns
        -------
        ShortcutDetermination
            Determination containing which new and existing relationships should be shortcuts
        """
        # Load existing child-parent relationships
        existing_relationships = await self._load_child_parent_relationships()

        # Convert new requests to models for validation
        new_relationship_models = {
            data.id: RelationshipInfoModel(**data.model_dump(by_alias=True))
            for data in new_relationships
        }

        # Validate all relationships together
        all_relationships = list(existing_relationships.values()) + list(
            new_relationship_models.values()
        )
        validation_result = await self.relationship_info_validation_service.validate_relationships(
            all_relationships
        )

        # Classify shortcuts
        determination = ShortcutDetermination()
        for relationship_id in validation_result.unused_relationship_info_ids:
            if relationship_id in existing_relationships:
                determination.existing_shortcuts.add(relationship_id)
            else:
                determination.new_shortcuts.add(relationship_id)

        return determination

    async def _load_child_parent_relationships(self) -> dict[ObjectId, RelationshipInfoModel]:
        """
        Load all existing CHILD_PARENT and CHILD_PARENT_SHORTCUT relationships.

        Returns
        -------
        dict[ObjectId, RelationshipInfoModel]
            Dictionary mapping relationship IDs to RelationshipInfoModel objects
        """
        relationships = {}
        async for rel_info in self.relationship_info_service.list_documents_iterator(
            query_filter={
                "relationship_type": {
                    "$in": [RelationshipType.CHILD_PARENT, RelationshipType.CHILD_PARENT_SHORTCUT]
                }
            },
        ):
            relationships[rel_info.id] = rel_info
        return relationships

    async def _create_new_relationships(
        self,
        new_relationships: list[RelationshipInfoCreate],
        shortcut_determination: ShortcutDetermination,
    ) -> None:
        """
        Create new relationship infos, updating entity ancestor_ids as needed.

        Parameters
        ----------
        new_relationships : list[RelationshipInfoCreate]
            List of new relationship creation requests
        shortcut_determination : ShortcutDetermination
            Determination of which relationships should be shortcuts
        """
        for data in new_relationships:
            if data.id in shortcut_determination.new_shortcuts:
                # This is a shortcut relationship, create without updating ancestor_ids since it
                # should already be up to date through the longer relationships path
                data = data.model_copy()
                data.relationship_type = RelationshipType.CHILD_PARENT_SHORTCUT
            else:
                # Update entity ancestor_ids
                await self._add_parent_to_child_entity(
                    child_id=data.entity_id,
                    parent_id=data.related_entity_id,
                    relation_table_id=data.relation_table_id,
                )

            await self.relationship_info_service.create_document(data)

    async def _add_parent_to_child_entity(
        self,
        child_id: ObjectId,
        parent_id: ObjectId,
        relation_table_id: ObjectId,
    ) -> None:
        """
        Add a parent relationship to a child entity, updating ancestor_ids.

        Parameters
        ----------
        child_id : ObjectId
            The child entity ID
        parent_id : ObjectId
            The parent entity ID
        relation_table_id : ObjectId
            The relation table ID linking the entities
        """
        table_model = await self.table_service.get_document(document_id=relation_table_id)
        await self.entity_relationship_service.add_relationship(
            parent=ParentEntity(
                id=parent_id,
                table_id=relation_table_id,
                table_type=table_model.type,
            ),
            child_id=child_id,
            skip_validation=True,  # Already validated above
        )

    async def _mark_existing_as_shortcuts(self, shortcut_ids: set[ObjectId]) -> None:
        """
        Mark existing relationships as CHILD_PARENT_SHORTCUT.

        Parameters
        ----------
        shortcut_ids : set[ObjectId]
            Set of relationship IDs to mark as shortcuts
        """
        for relationship_id in shortcut_ids:
            await self.relationship_info_service.update_document(
                document_id=relationship_id,
                data=RelationshipInfoServiceUpdate(
                    relationship_type=RelationshipType.CHILD_PARENT_SHORTCUT
                ),
            )

    async def _create_conflict_relationships(
        self,
        conflicts: list[RelationshipInfoCreate],
    ) -> None:
        """
        Create relationship infos with CONFLICT status for detected cycles.

        Parameters
        ----------
        conflicts : list[RelationshipInfoCreate]
            List of conflicting relationship creation requests
        """
        for conflict_data in conflicts:
            conflict_data = conflict_data.model_copy()
            conflict_data.relationship_type = RelationshipType.DISABLED

            relationship_info = await self.relationship_info_service.create_document(conflict_data)

            # Mark as conflict (requires separate update due to status not in Create schema)
            await self.relationship_info_service.update_document(
                document_id=relationship_info.id,
                data=RelationshipInfoServiceUpdate(relationship_status=RelationshipStatus.CONFLICT),
            )

    async def _mark_relationships_for_review(self, relationship_ids: set[ObjectId]) -> None:
        """
        Mark relationships as TO_REVIEW when new relation table candidates are found.

        Parameters
        ----------
        relationship_ids : set[ObjectId]
            Set of relationship IDs to mark for review
        """
        for relationship_id in relationship_ids:
            await self.relationship_info_service.update_document(
                document_id=relationship_id,
                data=RelationshipInfoServiceUpdate(
                    relationship_status=RelationshipStatus.TO_REVIEW
                ),
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

        Raises
        ------
        DocumentUpdateError
            If relation table ID is not valid for the given entity pair
        """
        relationship_info = await self.relationship_info_service.get_document(
            document_id=relationship_info_id
        )

        # Handle relationship type change separately
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

        # RelationshipInfoServiceUpdate contains internal fields not directly exposed to users
        update_data = RelationshipInfoServiceUpdate(**data.model_dump(exclude_unset=True))

        # Handle relation table change
        if data.relation_table_id is not None:
            relation_tables = await self.get_relation_tables(
                entity_id=relationship_info.entity_id,
                related_entity_id=relationship_info.related_entity_id,
            )
            for rel_table in relation_tables:
                if rel_table.relation_table_id == data.relation_table_id:
                    # Found valid relation table, proceed with update
                    break
            else:
                raise DocumentUpdateError(
                    f"Relation table ID {data.relation_table_id} is not a valid relation table "
                    "for the given entity pair"
                )
            update_data.relation_table_id = rel_table.relation_table_id
            update_data.entity_column_name = rel_table.entity_column_name
            update_data.related_entity_column_name = rel_table.related_entity_column_name

        updated_relationship_info = await self.relationship_info_service.update_document(
            document_id=relationship_info_id, data=update_data
        )

        return cast(RelationshipInfoModel, updated_relationship_info)
