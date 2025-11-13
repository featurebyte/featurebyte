"""
Tests for RelationshipInfoValidationService
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import InvalidEntityRelationshipError
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.entity import EntityCreate
from featurebyte.service.relationship_info_validation import RelationshipInfoValidationService


@pytest.fixture(name="service")
def service_fixture(app_container) -> RelationshipInfoValidationService:
    """
    Fixture for RelationshipInfoValidationService
    """
    return app_container.relationship_info_validation_service


async def create_entity(app_container, _id, name):
    """
    Create an entity
    """
    entity_model = await app_container.entity_service.create_document(
        EntityCreate(
            _id=_id,
            name=name,
            serving_name=f"{name}_serving",
        )
    )
    return entity_model.id


@pytest_asyncio.fixture(name="entity_a")
async def entity_a_fixture(app_container):
    """
    Fixture for entity_a
    """
    return await create_entity(app_container, ObjectId("60f7f9f5d2b5c12a3456789a"), "entity_a")


@pytest_asyncio.fixture(name="entity_b")
async def entity_b_fixture(app_container):
    """
    Fixture for entity_b
    """
    return await create_entity(app_container, ObjectId("60f7f9f5d2b5c12a3456789b"), "entity_b")


@pytest_asyncio.fixture(name="entity_c")
async def entity_c_fixture(app_container):
    """
    Fixture for entity_c
    """
    return await create_entity(app_container, ObjectId("60f7f9f5d2b5c12a3456789c"), "entity_c")


@pytest_asyncio.fixture(name="entity_d")
async def entity_d_fixture(app_container):
    """
    Fixture for entity_d
    """
    return await create_entity(app_container, ObjectId("60f7f9f5d2b5c12a3456789d"), "entity_d")


@pytest_asyncio.fixture(name="entity_e")
async def entity_e_fixture(app_container):
    """
    Fixture for entity_e
    """
    return await create_entity(app_container, ObjectId("60f7f9f5d2b5c12a3456789e"), "entity_e")


@pytest.fixture(name="relationship_a_to_b")
def relationship_a_to_b_fixture(entity_a, entity_b, catalog_id):
    """
    Fixture for relationship_info_a
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3ab",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_a,
        related_entity_id=entity_b,
        relation_table_id="69136faca6be526b342aa1aa",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_a_to_c")
def relationship_a_to_c_fixture(entity_a, entity_c, catalog_id):
    """
    Fixture for relationship_info_a_to_c
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3ac",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_a,
        related_entity_id=entity_c,
        relation_table_id="69136faca6be526b342aa1aa",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_a_to_d")
def relationship_a_to_d_fixture(entity_a, entity_d, catalog_id):
    """
    Fixture for relationship_info_a_to_d
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3ad",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_a,
        related_entity_id=entity_d,
        relation_table_id="69136faca6be526b342aa1aa",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_b_to_c")
def relationship_b_to_c_fixture(entity_b, entity_c, catalog_id):
    """
    Fixture for relationship_info_b_to_c
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3bc",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_b,
        related_entity_id=entity_c,
        relation_table_id="69136faca6be526b342aa1bb",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_d_to_c")
def relationship_d_to_c_fixture(entity_d, entity_c, catalog_id):
    """
    Fixture for relationship_info_d_to_c
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3dc",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_d,
        related_entity_id=entity_c,
        relation_table_id="69136faca6be526b342aa1dd",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_c_to_d")
def relationship_c_to_d_fixture(entity_c, entity_d, catalog_id):
    """
    Fixture for relationship_info_c_to_d
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3cd",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_c,
        related_entity_id=entity_d,
        relation_table_id="69136faca6be526b342aa1cc",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_d_to_e")
def relationship_d_to_e_fixture(entity_d, entity_e, catalog_id):
    """
    Fixture for relationship_info_d_to_e
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3de",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_d,
        related_entity_id=entity_e,
        relation_table_id="69136faca6be526b342aa1dd",
        catalog_id=catalog_id,
        enabled=True,
    )


@pytest.fixture(name="relationship_c_to_e")
def relationship_c_to_e_fixture(entity_c, entity_e, catalog_id):
    """
    Fixture for relationship_info_c_to_e
    """
    return RelationshipInfoModel(
        _id="69136faca6be526b342aa3ce",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=entity_c,
        related_entity_id=entity_e,
        relation_table_id="69136faca6be526b342aa1cc",
        catalog_id=catalog_id,
        enabled=True,
    )


def check_all_entity_pair_lookup_info(all_entity_pair_lookup_info, expected):
    """
    Check all entity pair lookup info
    """
    mapping = {
        (info.from_entity_id, info.to_entity_id): info.relationship_info_ids
        for info in all_entity_pair_lookup_info
    }
    assert mapping == expected


@pytest.mark.asyncio
async def test_single_relationship_info(service, relationship_a_to_b, entity_a, entity_b):
    """
    Test single relationship info

    A -> B
    """
    result = await service.validate_relationships([relationship_a_to_b])
    assert len(result.all_entity_pair_lookup_info) == 1
    expected = {
        (entity_a, entity_b): [relationship_a_to_b.id],
    }
    check_all_entity_pair_lookup_info(result.all_entity_pair_lookup_info, expected)
    assert result.unused_relationship_info_ids == []


@pytest.mark.asyncio
async def test_multiple_relationship_infos__valid(
    service,
    relationship_a_to_b,
    relationship_a_to_c,
    relationship_b_to_c,
    entity_a,
    entity_b,
    entity_c,
):
    """
    Test multiple relationship infos

    A -> B -> C
    |         ^
    |         |
    +---------+

    A to C is a shortcut and is allowed.
    """
    result = await service.validate_relationships([
        relationship_a_to_b,
        relationship_a_to_c,
        relationship_b_to_c,
    ])
    assert len(result.all_entity_pair_lookup_info) == 3
    expected = {
        (entity_a, entity_b): [relationship_a_to_b.id],
        (entity_a, entity_c): [relationship_a_to_b.id, relationship_b_to_c.id],
        (entity_b, entity_c): [relationship_b_to_c.id],
    }
    check_all_entity_pair_lookup_info(result.all_entity_pair_lookup_info, expected)
    assert result.unused_relationship_info_ids == [relationship_a_to_c.id]


@pytest.mark.asyncio
async def test_multiple_relationship_infos__invalid(
    service,
    relationship_a_to_b,
    relationship_b_to_c,
    relationship_a_to_d,
    relationship_d_to_c,
):
    """
    Test multiple relationship infos (invalid)

    A -> B -> C
    |         ^
    v         |
    D --------+

    There are multiple distinct paths from A to C which is not allowed.
    * A -> B -> C
    * A -> D -> C
    """
    with pytest.raises(InvalidEntityRelationshipError) as exc:
        _ = await service.validate_relationships([
            relationship_a_to_b,
            relationship_b_to_c,
            relationship_a_to_d,
            relationship_d_to_c,
        ])
    expected_error = "Invalid entity tagging detected between entity_a (60f7f9f5d2b5c12a3456789a) and entity_c (60f7f9f5d2b5c12a3456789c). Please review the entities and their relationships in the catalog."
    assert str(exc.value) == expected_error


@pytest.mark.asyncio
async def test_shortcut_with_skipped_connections(
    service,
    relationship_a_to_b,
    relationship_b_to_c,
    relationship_a_to_c,
    relationship_a_to_d,
    relationship_c_to_d,
    relationship_c_to_e,
    relationship_d_to_e,
    entity_a,
    entity_b,
    entity_c,
    entity_d,
    entity_e,
):
    """
    Test shortcut with skipped connections

     --------------|
    |              v
    A -> B -> C -> D -> E
    |         ^         ^
    ----------+         |
              ----------+

    Multiple paths from A to E:
    * A -> B -> C -> D -> E
    * A -> B -> C -> E
    * A -> C -> D -> E
    * A -> C -> E
    * A -> D -> E

    All shortcuts are valid because they are shortcuts of the longest path A->B->C->D->E
    """
    result = await service.validate_relationships([
        relationship_a_to_b,
        relationship_b_to_c,
        relationship_a_to_c,
        relationship_a_to_d,
        relationship_c_to_d,
        relationship_c_to_e,
        relationship_d_to_e,
    ])

    # Check that we have the expected entity pair lookup infos (only valid paths)
    assert len(result.all_entity_pair_lookup_info) == 10

    # Build mapping for easier testing
    mapping = {
        (info.from_entity_id, info.to_entity_id): info.relationship_info_ids
        for info in result.all_entity_pair_lookup_info
    }

    # Test some key paths to verify shortcuts are handled correctly
    # A to E should use the full path: A -> B -> C -> D -> E
    assert mapping[(entity_a, entity_e)] == [
        relationship_a_to_b.id,
        relationship_b_to_c.id,
        relationship_c_to_d.id,
        relationship_d_to_e.id,
    ]

    # A to D should use the full path: A -> B -> C -> D (not the shortcut A -> D)
    assert mapping[(entity_a, entity_d)] == [
        relationship_a_to_b.id,
        relationship_b_to_c.id,
        relationship_c_to_d.id,
    ]

    # A to C should use the path: A -> B -> C (not the shortcut A -> C)
    assert mapping[(entity_a, entity_c)] == [relationship_a_to_b.id, relationship_b_to_c.id]

    # The shortcut relationships should be marked as unused
    assert set(result.unused_relationship_info_ids) == {
        relationship_a_to_c.id,
        relationship_a_to_d.id,
        relationship_c_to_e.id,
    }
