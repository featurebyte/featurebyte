"""
Primary entity mixin
"""

from abc import abstractmethod
from typing import List, Sequence

from bson import ObjectId

from featurebyte.api.api_object import ApiObject
from featurebyte.api.entity import Entity
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.relationship_analysis import derive_primary_entity


class PrimaryEntityMixin(ApiObject):
    """
    Mixin class containing common methods for primary entity classes

    The underlying model must have the following fields:
    - primary_entity_ids
    """

    @property
    @abstractmethod
    def entity_ids(self) -> Sequence[ObjectId]:
        """
        Returns the entity ids of the object.

        Returns
        -------
        Sequence[ObjectId]
        """

    @property
    @abstractmethod
    def primary_entity_ids(self) -> Sequence[ObjectId]:
        """
        Returns the primary entity ids of the object.

        Returns
        -------
        Sequence[ObjectId]
        """

    @property
    @abstractmethod
    def primary_entity(self) -> List[Entity]:
        """
        Returns the primary entity of the object.

        Returns
        -------
        List[Entity]
        """

    def _get_entities(self) -> List[Entity]:
        return [Entity.get_by_id(entity_id) for entity_id in self.entity_ids]

    def _get_primary_entity(self) -> List[Entity]:
        try:
            return [
                Entity.get_by_id(entity_id) for entity_id in self.cached_model.primary_entity_ids
            ]
        except RecordRetrievalException:
            entities = []
            for entity_id in self.entity_ids:
                entities.append(Entity.get_by_id(entity_id))
            return derive_primary_entity(entities)  # type: ignore

    def _get_primary_entity_ids(self) -> Sequence[ObjectId]:
        try:
            return self.cached_model.primary_entity_ids
        except RecordRetrievalException:
            return sorted([entity.id for entity in self.primary_entity])
