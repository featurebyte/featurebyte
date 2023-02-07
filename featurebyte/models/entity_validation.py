"""
Models related to entity validation
"""
from __future__ import annotations

from typing import List

from bson import ObjectId
from pydantic import validator

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel


class EntityInfo(FeatureByteBaseModel):
    """
    EntityInfo captures the entity information provided in the request

    required_entities: List[EntityModel]
        List of entities required to by the feature / feature list
    available_entities: List[EntityModel]
        List of entities provided in the request
    """

    required_entities: List[EntityModel]
    provided_entities: List[EntityModel]

    @validator("required_entities", "provided_entities")
    @classmethod
    def _deduplicate_entities(cls, val: List[EntityModel]) -> List[EntityModel]:
        entities_dict: dict[ObjectId, EntityModel] = {}
        for entity in val:
            entities_dict[entity.id] = entity
        return list(entities_dict.values())

    def are_all_required_entities_provided(self) -> bool:
        """
        Returns whether all the required entities are provided in the request

        Returns
        -------
        bool
        """
        return self.required_entity_ids <= self.provided_entity_ids

    @property
    def required_entity_ids(self) -> set[ObjectId]:
        """
        Set of the required entity ids

        Returns
        -------
        set[ObjectId]
        """
        return {entity.id for entity in self.required_entities}

    @property
    def provided_entity_ids(self) -> set[ObjectId]:
        """
        Set of provided entity ids

        Returns
        -------
        set[ObjectId]
        """
        return {entity.id for entity in self.provided_entities}

    @property
    def missing_entities(self) -> List[EntityModel]:
        """
        List of entities that are required but not provided

        Returns
        -------
        List[EntityModel]
        """
        provided_ids = self.provided_entity_ids
        return [entity for entity in self.required_entities if entity.id not in provided_ids]
