"""
Test entity relationship extractor
"""
# pylint: disable=too-many-arguments,anomalous-backslash-in-string
import pytest

from featurebyte.service.entity_relationship_extractor import ServingEntityEnumeration


@pytest.fixture(name="entity_relationship_extractor")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.entity_relationship_extractor_service


@pytest.mark.asyncio
async def test_extract_relationship_from_primary_entity_case_1(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    relationship_father_son,
    relationship_mother_son,
    grandpa_entity_id,
    granny_entity_id,
    father_entity_id,
    mother_entity_id,
    son_entity_id,
):
    """Test extract_relationship_from_primary_entity (case 1)"""
    extractor = entity_relationship_extractor
    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[grandpa_entity_id, granny_entity_id, son_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {
        relationship_grandpa_father.id,
        relationship_granny_father.id,
        relationship_father_son.id,
    }

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[granny_entity_id, son_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_granny_father.id, relationship_father_son.id}

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[granny_entity_id, father_entity_id, son_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_granny_father.id, relationship_father_son.id}

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[father_entity_id, mother_entity_id, son_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_father_son.id, relationship_mother_son.id}

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[son_entity_id],
    )
    assert output == []


@pytest.mark.asyncio
async def test_extract_relationship_from_primary_entity_case_2(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    grandpa_entity_id,
    granny_entity_id,
    father_entity_id,
    mother_entity_id,
):
    """Test extract_relationship_from_primary_entity (case 2)"""
    extractor = entity_relationship_extractor
    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[father_entity_id, mother_entity_id],
    )
    assert output == []

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[granny_entity_id, mother_entity_id],
    )
    assert output == []

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[granny_entity_id, father_entity_id, mother_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_granny_father.id}

    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=[grandpa_entity_id, father_entity_id]
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_grandpa_father.id}


@pytest.mark.asyncio
async def test_extract_primary_entity_descendant_relationship__case_1(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    relationship_father_son,
    relationship_mother_son,
    grandpa_entity_id,
    granny_entity_id,
    father_entity_id,
    mother_entity_id,
    son_entity_id,
):
    """Test extract_primary_entity_descendant_relationship (case 1)"""
    extractor = entity_relationship_extractor
    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[grandpa_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_grandpa_father.id, relationship_father_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[granny_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_granny_father.id, relationship_father_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[father_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_father_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[mother_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_mother_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[father_entity_id, mother_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_father_son.id, relationship_mother_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[son_entity_id],
    )
    assert output == []


@pytest.mark.asyncio
async def test_extract_primary_entity_descendant_relationship__case_2(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_mother_son,
    grandpa_entity_id,
    father_entity_id,
    mother_entity_id,
    son_entity_id,
):
    """Test extract_primary_entity_descendant_relationship (case 2)"""
    extractor = entity_relationship_extractor
    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[grandpa_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_grandpa_father.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[father_entity_id],
    )
    assert output == []

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[mother_entity_id],
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_mother_son.id}

    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=[son_entity_id],
    )
    assert output == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entities,expected_serving_entities",
    [
        ([], [[]]),
        (["grandpa"], [["grandpa"], ["father"], ["son"]]),
        (["grandpa", "father"], [["father"], ["son"]]),
        (["grandpa", "son"], [["son"]]),
        (["granny", "mother"], [["granny", "mother"], ["father", "mother"], ["son"]]),
        (["grandpa", "granny"], [["grandpa", "granny"], ["father"], ["son"]]),
        (["father"], [["father"], ["son"]]),
        (["mother"], [["mother"], ["son"]]),
        (["mother", "son"], [["son"]]),
        (["father", "mother"], [["father", "mother"], ["son"]]),
    ],
)
async def test_enumerate_all_serving_entity_ids(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    relationship_father_son,
    relationship_mother_son,
    grandpa_entity_id,
    granny_entity_id,
    father_entity_id,
    mother_entity_id,
    son_entity_id,
    entities,
    expected_serving_entities,
):
    """
    Test enumerate_all_serving_entity_ids

    === Family Tree ===
     grandpa  granny
        \     /
        father    mother
            \     /
              son
    """
    _ = (
        relationship_grandpa_father,
        relationship_granny_father,
        relationship_father_son,
        relationship_mother_son,
    )
    extractor = entity_relationship_extractor
    entity_map = {
        "grandpa": grandpa_entity_id,
        "granny": granny_entity_id,
        "father": father_entity_id,
        "mother": mother_entity_id,
        "son": son_entity_id,
    }
    primary_entity_ids = [entity_map[entity] for entity in entities]
    relationships_info = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=primary_entity_ids,
    )
    serving_entity_enumeration = ServingEntityEnumeration.create(
        relationships_info=relationships_info
    )
    all_serving_entity_ids = serving_entity_enumeration.generate(entity_ids=primary_entity_ids)
    output = set(tuple(entity_ids) for entity_ids in all_serving_entity_ids)
    expected_serving_entities = set(
        tuple(entity_map[entity] for entity in entities) for entities in expected_serving_entities
    )
    assert output == expected_serving_entities
