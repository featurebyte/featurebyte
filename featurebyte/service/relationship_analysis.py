"""
RelationshipAnalysisService class
"""
from __future__ import annotations

from typing import List

from featurebyte.models.entity import EntityModel


def derive_primary_entity(entities: List[EntityModel]) -> List[EntityModel]:
    """
    Derive the primary entity from a list of entities

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
    primary_entity = {}
    for entity in entities:
        if entity.id not in all_ancestors_ids:
            primary_entity[entity.id] = entity

    return sorted(primary_entity.values(), key=lambda e: e.id)
