"""
Unit tests for EntityService
"""

import pytest
from bson import ObjectId


def _sorted_entities(entities):
    """
    Utility to sort list of entities for easier comparison
    """
    return sorted(entities, key=lambda x: x.name)


@pytest.mark.asyncio
async def test_entity_service__get_entities_with_serving_names(entity_a, entity_b, entity_service):
    """
    Test retrieving entities with serving names
    """
    assert await entity_service.get_entities_with_serving_names({"non_existing"}) == []
    assert await entity_service.get_entities_with_serving_names({"A"}) == [entity_a]
    assert await entity_service.get_entities_with_serving_names({"B"}) == [entity_b]
    assert _sorted_entities(
        await entity_service.get_entities_with_serving_names({"A", "B"})
    ) == _sorted_entities([
        entity_a,
        entity_b,
    ])


@pytest.mark.asyncio
async def test_entity_service__get_entities(entity_a, entity_b, entity_service):
    """
    Test retrieving a list of entities
    """
    entities = await entity_service.get_entities({entity_a.id, entity_b.id})
    assert _sorted_entities(entities) == _sorted_entities([entity_a, entity_b])

    entities = await entity_service.get_entities({ObjectId()})
    assert entities == []
