"""
Module to support serving parent features using child entities
"""
from __future__ import annotations

from typing import List, Tuple

from collections import OrderedDict

from bson import ObjectId

from featurebyte.exception import AmbiguousEntityRelationshipError, EntityJoinPathNotFoundError
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import JoinStep
from featurebyte.service.entity import EntityService
from featurebyte.service.table import TableService


class ParentEntityLookupService:
    """
    ParentEntityLookupService is responsible for identifying the joins required to lookup parent
    entities in order to serve parent features given child entities
    """

    def __init__(
        self,
        entity_service: EntityService,
        table_service: TableService,
    ):
        self.entity_service = entity_service
        self.table_service = table_service

    async def get_required_join_steps(self, entity_info: EntityInfo) -> list[JoinStep]:
        """
        Get the list of required JoinStep to lookup the missing entities in the request

        Parameters
        ----------
        entity_info: EntityInfo
            Entity information

        Returns
        -------
        list[JoinStep]
        """

        if entity_info.are_all_required_entities_provided():
            return []

        # all_join_steps is a mapping from parent serving name to JoinStep. Each parent serving name
        # should be looked up exactly once and then reused.
        all_join_steps: dict[str, JoinStep] = OrderedDict()
        current_available_entities = entity_info.provided_entity_ids

        for entity in entity_info.missing_entities:
            join_path = await self._get_entity_join_path(entity, current_available_entities)

            # Extract list of JoinStep
            join_steps = await self._get_join_steps_from_join_path(entity_info, join_path)
            for join_step in join_steps:
                if join_step.parent_serving_name not in all_join_steps:
                    all_join_steps[join_step.parent_serving_name] = join_step

            # All the entities in the path are now available
            current_available_entities.update([entity.id for entity in join_path])

        return list(all_join_steps.values())

    async def _get_join_steps_from_join_path(
        self, entity_info: EntityInfo, join_path: list[EntityModel]
    ) -> list[JoinStep]:
        """
        Convert a list of join path (list of EntityModel) into a list of JoinStep

        Parameters
        ----------
        entity_info: EntityInfo
            Entity information
        join_path: list[EntityModel]
            A list of related entities from a given entity to a target entity

        Returns
        -------
        list[JoinStep]
        """

        join_steps = []

        # Retrieve all entities in batch
        all_entities = await self.entity_service.get_entities({entity.id for entity in join_path})
        entities_by_id = {entity.id: entity for entity in all_entities}

        for child_entity, parent_entity in zip(join_path, join_path[1:]):
            child_entity_id = child_entity.id
            parent_entity_id = parent_entity.id

            # Retrieve the relationship for the table id defined in the relationship
            parents = entities_by_id[child_entity_id].parents
            relationship = next(parent for parent in parents if parent.id == parent_entity_id)

            # Retrieve the join keys from the table
            data = await self.table_service.get_document(relationship.table_id)
            child_key, parent_key = None, None
            for column_info in data.columns_info:
                name = column_info.name
                if column_info.entity_id == child_entity_id:
                    child_key = name
                if column_info.entity_id == parent_entity_id:
                    parent_key = name

            assert child_key is not None
            assert parent_key is not None

            # Converting table to dict by_alias to preserve the correct id when constructing JoinStep
            join_step = JoinStep(
                table=data.dict(by_alias=True),
                parent_key=parent_key,
                parent_serving_name=entity_info.get_effective_serving_name(parent_entity),
                child_key=child_key,
                child_serving_name=entity_info.get_effective_serving_name(child_entity),
            )
            join_steps.append(join_step)

        return join_steps

    async def _get_entity_join_path(
        self,
        required_entity: EntityModel,
        available_entity_ids: set[ObjectId],
    ) -> list[EntityModel]:
        """
        Get a join path given a required entity (missing but required for feature generation) and
        a list of provided entities.

        The result is a list of entities where each pair of adjacent entities are related to each
        other. A child appears before its parent in this list.

        Parameters
        ----------
        required_entity: EntityModel
            Required entity
        available_entity_ids: set[ObjectId]
            List of currently available entities

        Returns
        -------
        list[EntityModel]

        Raises
        ------
        EntityJoinPathNotFoundError
            If a join path cannot be identified
        AmbiguousEntityRelationshipError
            If no unique join path can be identified due to ambiguous relationships
        """
        pending: List[Tuple[EntityModel, List[EntityModel]]] = [(required_entity, [])]
        queued = set()
        result = None

        while pending:
            (current_entity, current_path), pending = pending[0], pending[1:]
            updated_path = [current_entity] + current_path

            if current_entity.id in available_entity_ids and result is None:
                # Do not exit early, continue to see if there are multiple join paths (can be
                # detected when an entity is queued more than once)
                result = updated_path

            children_entities = await self.entity_service.get_children_entities(current_entity.id)
            for child_entity in children_entities:
                if child_entity.name not in queued:
                    queued.add(child_entity.name)
                    pending.append((child_entity, updated_path))
                else:
                    # There should be only one way to obtain the parent entity. Raise an error
                    # otherwise.
                    raise AmbiguousEntityRelationshipError(
                        f"Cannot find an unambiguous join path for entity {required_entity.name}"
                    )

        if result is not None:
            return result

        raise EntityJoinPathNotFoundError(
            f"Cannot find a join path for entity {required_entity.name}"
        )
