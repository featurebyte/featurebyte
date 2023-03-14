"""
Models related to entity validation
"""
from __future__ import annotations

from typing import Dict, List, Optional

from bson import ObjectId
from pydantic import validator

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel


class EntityInfo(FeatureByteBaseModel):
    """
    EntityInfo captures the entity information provided in the request

    required_entities: List[EntityModel]
        List of entities required to by the feature / feature list
    provided_entities: List[EntityModel]
        List of entities provided in the request
    serving_names_mapping: Optional[Dict[str, str]]
        Optional mapping from original serving name to new serving names (used when the request
        wants to override the original serving names with new ones)
    """

    required_entities: List[EntityModel]
    provided_entities: List[EntityModel]
    serving_names_mapping: Optional[Dict[str, str]]

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

    def get_effective_serving_name(self, entity: EntityModel) -> str:
        """
        Get the serving name for the entity taking into account the serving names mapping, if any

        Parameters
        ----------
        entity: EntityModel
            Entity object

        Returns
        -------
        str
        """
        original_serving_name = entity.serving_names[0]
        if (
            self.serving_names_mapping is None
            or original_serving_name not in self.serving_names_mapping
        ):
            return original_serving_name
        return self.serving_names_mapping[original_serving_name]
