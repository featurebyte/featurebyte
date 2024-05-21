"""
RelationshipAnalysisService class
"""

from __future__ import annotations

from typing import List, TypeVar

from featurebyte.models.entity import EntityModel

EntityModelT = TypeVar("EntityModelT", bound=EntityModel)


def derive_primary_entity(entities: List[EntityModelT]) -> List[EntityModelT]:
    """
    Derive the primary entity from a list of entities

    Parameters
    ----------
    entities: List[EntityModelT]
        List of entities

    Returns
    -------
    List[EntityModelT]
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
