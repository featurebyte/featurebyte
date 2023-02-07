"""
Module to support serving parent features using child entities
"""
from __future__ import annotations

from typing import Any, List, Tuple

from collections import defaultdict

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
    ParentEntityLookupService is responsible for identifying the joins require to lookup parent
    entities in order to serve parent features given child entities
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.entity_service = EntityService(user, persistent)
        self.data_service = DataService(user, persistent)

    async def get_required_join_steps(self, entity_info: EntityInfo) -> list[JoinStep]:

        if entity_info.are_all_required_entities_provided():
            return []

        all_join_steps = []
        for entity in entity_info.missing_entities:
            join_path = await self.get_entity_join_path(entity, entity_info.provided_entities)
            join_steps = await self.get_join_steps_from_join_path(join_path)
            for join_step in join_steps:
                if join_step not in all_join_steps:
                    all_join_steps.append(join_step)

        return all_join_steps

    async def get_join_steps_from_join_path(self, join_path: list[EntityModel]) -> list[JoinStep]:

        join_steps = []

        for child_entity, parent_entity in zip(join_path, join_path[1:]):

            child_entity_id = child_entity.id
            parent_entity_id = parent_entity.id

            parents = (await self.entity_service.get_document(child_entity_id)).parents
            relationship = next(parent for parent in parents if parent.id == parent_entity_id)

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

    async def get_entity_join_path(
        self,
        required_entity: EntityModel,
        provided_entities: list[EntityModel],
    ) -> list[EntityModel]:

        pending: List[Tuple[EntityModel, List[EntityModel]]] = [(required_entity, [])]

        join_path = None
        visited = defaultdict(bool)

        provided_entity_ids = set([entity.id for entity in provided_entities])

        while pending:

            (current_entity, current_path), pending = pending[0], pending[1:]
            updated_path = [current_entity] + current_path

            if current_entity.id in provided_entity_ids:
                join_path = updated_path
                break

            visited[current_entity.id] = True
            children_entities = await self.entity_service.get_children_entities(current_entity.id)
            for child_entity in children_entities:
                if not visited[child_entity.id]:
                    pending.append((child_entity, updated_path))

        if join_path is None:
            raise EntityJoinPathNotFoundError(
                f"Cannot find a join path for entity {required_entity.name}"
            )

        return join_path
