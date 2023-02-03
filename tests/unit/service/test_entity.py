"""
Unit tests for EntityService
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.entity import ParentEntity
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate


@pytest_asyncio.fixture(name="entity_a")
async def entity_a_fixture(entity_service):
    """
    An entity A
    """
    entity_a = await entity_service.create_document(EntityCreate(name="entity_a", serving_name="a"))
    return entity_a


@pytest_asyncio.fixture(name="entity_b")
async def entity_b_fixture(entity_service):
    """
    An entity B
    """
    entity_b = await entity_service.create_document(EntityCreate(name="entity_b", serving_name="b"))
    return entity_b


@pytest_asyncio.fixture(name="entity_c")
async def entity_c_fixture(entity_service):
    """
    An entity C
    """
    entity_c = await entity_service.create_document(EntityCreate(name="entity_c", serving_name="c"))
    return entity_c


@pytest_asyncio.fixture(name="b_is_parent_of_a")
async def b_is_parent_of_a_fixture(entity_a, entity_b, event_data, entity_service):
    """
    Fixture to make B a parent of A
    """
    parent = ParentEntity(id=entity_b.id, data_type=event_data.type, data_id=event_data.id)
    update_entity_a = EntityServiceUpdate(parents=[parent])
    await entity_service.update_document(entity_a.id, update_entity_a)


@pytest_asyncio.fixture(name="c_is_parent_of_b")
async def c_is_parent_of_b_fixture(entity_b, entity_c, event_data, entity_service):
    """
    Fixture to make C a parent of B
    """
    parent = ParentEntity(id=entity_c.id, data_type=event_data.type, data_id=event_data.id)
    update_entity_b = EntityServiceUpdate(parents=[parent])
    await entity_service.update_document(entity_b.id, update_entity_b)


@pytest.fixture
def relationships(b_is_parent_of_a, c_is_parent_of_b):
    """
    Fixture to register all relationships
    """
    _ = b_is_parent_of_a
    _ = c_is_parent_of_b
    yield


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
    assert await entity_service.get_entities_with_serving_names({"a"}) == [entity_a]
    assert await entity_service.get_entities_with_serving_names({"b"}) == [entity_b]
    assert _sorted_entities(
        await entity_service.get_entities_with_serving_names({"a", "b"})
    ) == _sorted_entities([entity_a, entity_b])


@pytest.mark.asyncio
async def test_entity_service__get_entities(entity_a, entity_b, entity_service):
    """
    Test retrieving a list of entities
    """
    entities = await entity_service.get_entities({entity_a.id, entity_b.id})
    assert _sorted_entities(entities) == _sorted_entities([entity_a, entity_b])

    entities = await entity_service.get_entities({ObjectId()})
    assert entities == []


@pytest.mark.asyncio
async def test_entity_service__get_children(
    entity_a, entity_b, entity_c, entity_service, relationships
):
    """
    Test get_children (should get immediate children only)
    """
    _ = relationships

    children = await entity_service.get_children_entities(entity_b.id)
    assert [child.id for child in children] == [entity_a.id]

    children = await entity_service.get_children_entities(entity_c.id)
    assert [child.id for child in children] == [entity_b.id]
