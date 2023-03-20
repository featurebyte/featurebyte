"""
RelationshipAnalysisService class
"""
from __future__ import annotations

from typing import List

from featurebyte.models.entity import EntityModel


class RelationshipAnalysisService:
    """
    RelationshipAnalysisService is responsible for deriving feature or feature list attributes such
    as primary entities, serving entities, etc based on the relationships between entities.
    """

    @staticmethod
    def derive_primary_entities(entities: List[EntityModel]) -> List[EntityModel]:
        """
        Derive primary entities from a list of entities

        Parameters
        ----------
        entities: List[EntityModel]
            List of entities

        Returns
        -------
        List[EntityModel]
            List of main entities
        """

        all_ancestors_ids = set()
        for entity in entities:
            all_ancestors_ids.update(entity.ancestor_ids)

        # Primary entities are entities that are not ancestors of any other entities
        primary_entities = {}
        for entity in entities:
            if entity.id not in all_ancestors_ids:
                primary_entities[entity.id] = entity

        return sorted(primary_entities.values(), key=lambda e: e.id)
