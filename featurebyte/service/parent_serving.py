"""
Module to support serving parent features using child entities
"""
from __future__ import annotations

from typing import Any, List, Tuple

from collections import defaultdict

from bson import ObjectId

from featurebyte.exception import EntityJoinPathNotFoundError
from featurebyte.models.entity import EntityModel
from featurebyte.models.entity_validation import EntityInfo
from featurebyte.models.parent_serving import JoinStep
from featurebyte.persistent import Persistent
from featurebyte.service.base_service import BaseService
from featurebyte.service.entity import EntityService
from featurebyte.service.tabular_data import DataService


class ParentEntityLookupService(BaseService):
    """
    ParentEntityLookupService is responsible for identifying the joins required to lookup parent
    entities in order to serve parent features given child entities
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        entity_service: EntityService,
        data_service: DataService,
    ):
        super().__init__(user, persistent)
        self.entity_service = entity_service
        self.data_service = data_service

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

        all_join_steps = []
        current_available_entities = entity_info.provided_entity_ids

        for entity in entity_info.missing_entities:
            join_path = await self._get_entity_join_path(entity, current_available_entities)

            # Extract list of JoinStep
            join_steps = await self._get_join_steps_from_join_path(join_path)
            for join_step in join_steps:
                if join_step not in all_join_steps:
                    all_join_steps.append(join_step)

            # All the entities in the path are now available
            current_available_entities.update([entity.id for entity in join_path])

        return all_join_steps

    async def _get_join_steps_from_join_path(self, join_path: list[EntityModel]) -> list[JoinStep]:
        """
        Convert a list of join path (list of EntityModel) into a list of JoinStep

        Parameters
        ----------
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

            # Retrieve the relationship for the data id defined in the relationship
            parents = entities_by_id[child_entity_id].parents
            relationship = next(parent for parent in parents if parent.id == parent_entity_id)

            # Retrieve the join keys from the data
            data = await self.data_service.get_document(relationship.data_id)
            child_key, parent_key = None, None
            for column_info in data.columns_info:
                name = column_info.name
                if column_info.entity_id == child_entity_id:
                    child_key = name
                if column_info.entity_id == parent_entity_id:
                    parent_key = name

            assert child_key is not None
            assert parent_key is not None

            # Converting data to dict by_alias to preserve the correct id when constructing JoinStep
            join_step = JoinStep(
                data=data.dict(by_alias=True),
                parent_key=parent_key,
                parent_serving_name=parent_entity.serving_names[0],
                child_key=child_key,
                child_serving_name=child_entity.serving_names[0],
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
        """
        # Perform a BFS traversal from the required entity and stop once any of the available
        # entities is reached. This should be the fastest way to join datas to obtain the required
        # entity (requiring the least number of joins) assuming all joins have the same cost.
        pending: List[Tuple[EntityModel, List[EntityModel]]] = [(required_entity, [])]
        visited = defaultdict(bool)

        while pending:

            (current_entity, current_path), pending = pending[0], pending[1:]
            updated_path = [current_entity] + current_path

            if current_entity.id in available_entity_ids:
                return updated_path

            visited[current_entity.id] = True
            children_entities = await self.entity_service.get_children_entities(current_entity.id)
            for child_entity in children_entities:
                if not visited[child_entity.id]:
                    pending.append((child_entity, updated_path))

        raise EntityJoinPathNotFoundError(
            f"Cannot find a join path for entity {required_entity.name}"
        )
