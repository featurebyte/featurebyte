"""
Tests for RelationshipInfoValidationService
"""

import pytest
from bson import ObjectId

from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for RelationshipInfoValidationService
    """
    return app_container.relationship_info_validation_service


@pytest.fixture(name="entity_a")
def entity_a_fixture():
    """
    Fixture for entity_a
    """
    return ObjectId("60f7f9f5d2b5c12a3456789a")


@pytest.fixture(name="entity_b")
def entity_b_fixture():
    """
    Fixture for entity_b
    """
    return ObjectId("60f7f9f5d2b5c12a3456789b")


@pytest.fixture(name="entity_c")
def entity_c_fixture():
    """
    Fixture for entity_c
    """
    return ObjectId("60f7f9f5d2b5c12a3456789c")


@pytest.fixture(name="entity_d")
def entity_d_fixture():
    """
    Fixture for entity_d
    """
    return ObjectId("60f7f9f5d2b5c12a3456789d")


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
    Fixture for relationship_info_a
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
    Fixture for relationship_info_b_to_c
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


def check_all_entity_pair_lookup_info(all_entity_pair_lookup_info, expected):
    """
    Check all entity pair lookup info
    """
    mapping = {
        (info.from_entity_id, info.to_entity_id): info.relationship_info_ids
        for info in all_entity_pair_lookup_info
    }
    assert mapping == expected


def test_single_relationship_info(service, relationship_a_to_b, entity_a, entity_b):
    """
    Test single relationship info
    """
    result = service.validate_relationships([relationship_a_to_b])
    assert len(result.all_entity_pair_lookup_info) == 1
    expected = {
        (entity_a, entity_b): [relationship_a_to_b.id],
    }
    check_all_entity_pair_lookup_info(result.all_entity_pair_lookup_info, expected)
    assert result.unused_relationship_info_ids == []


def test_multiple_relationship_infos__valid(
    service,
    relationship_a_to_b,
    relationship_a_to_c,
    relationship_b_to_c,
    entity_a,
    entity_b,
    entity_c,
):
    """
    Test single relationship info
    """
    result = service.validate_relationships([
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


def test_multiple_relationship_infos__invalid(
    service,
    relationship_a_to_b,
    relationship_b_to_c,
    relationship_a_to_d,
    relationship_d_to_c,
):
    """
    Test single relationship info
    """
    with pytest.raises(ValueError) as exc:
        _ = service.validate_relationships([
            relationship_a_to_b,
            relationship_b_to_c,
            relationship_a_to_d,
            relationship_d_to_c,
        ])
    expected_error = "Invalid relationship paths between entities 60f7f9f5d2b5c12a3456789a and 60f7f9f5d2b5c12a3456789c."
    assert str(exc.value) == expected_error
