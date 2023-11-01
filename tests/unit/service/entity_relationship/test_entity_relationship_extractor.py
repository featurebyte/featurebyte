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
@pytest.mark.parametrize(
    "entities,expected_relationships",
    [
        ([], []),
        (["son"], []),
        (["grandpa", "granny", "son"], ["grandpa_father", "granny_father", "father_son"]),
        (["granny", "son"], ["granny_father", "father_son"]),
        (["granny", "father", "son"], ["granny_father", "father_son"]),
        (["father", "mother", "son"], ["father_son", "mother_son"]),
    ],
)
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
    entities,
    expected_relationships,
):
    """
    Test extract_relationship_from_primary_entity (case 1)

    === Family Tree ===
     grandpa  granny
        \     /
        father    mother
            \     /
              son
    """
    extractor = entity_relationship_extractor
    id_map = {
        # relationship
        "grandpa_father": relationship_grandpa_father.id,
        "granny_father": relationship_granny_father.id,
        "father_son": relationship_father_son.id,
        "mother_son": relationship_mother_son.id,
        # entity
        "grandpa": grandpa_entity_id,
        "granny": granny_entity_id,
        "father": father_entity_id,
        "mother": mother_entity_id,
        "son": son_entity_id,
    }
    entity_ids = [id_map[entity] for entity in entities]
    expected_relationship_ids = set(id_map[relationship] for relationship in expected_relationships)
    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=entity_ids,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == expected_relationship_ids


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entities,expected_relationships",
    [
        (["father", "mother"], []),
        (["granny", "mother"], []),
        (["granny", "father", "mother"], ["granny_father"]),
        (["grandpa", "father"], ["grandpa_father"]),
    ],
)
async def test_extract_relationship_from_primary_entity_case_2(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    grandpa_entity_id,
    granny_entity_id,
    father_entity_id,
    mother_entity_id,
    entities,
    expected_relationships,
):
    """
    Test extract_relationship_from_primary_entity (case 2)

    === Family Tree ===
     grandpa  granny
        \     /
        father    mother
    """
    extractor = entity_relationship_extractor
    id_map = {
        # relationship
        "grandpa_father": relationship_grandpa_father.id,
        "granny_father": relationship_granny_father.id,
        # entity
        "grandpa": grandpa_entity_id,
        "granny": granny_entity_id,
        "father": father_entity_id,
        "mother": mother_entity_id,
    }
    entity_ids = [id_map[entity] for entity in entities]
    expected_relationship_ids = set(id_map[relationship] for relationship in expected_relationships)
    output = await extractor.extract_relationship_from_primary_entity(
        entity_ids=entity_ids,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == expected_relationship_ids


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entities,expected_relationships",
    [
        (["grandpa"], ["grandpa_father", "father_son"]),
        (["granny"], ["granny_father", "father_son"]),
        (["father"], ["father_son"]),
        (["mother"], ["mother_son"]),
        (["father", "mother"], ["father_son", "mother_son"]),
        (["son"], []),
        ([], []),
    ],
)
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
    entities,
    expected_relationships,
):
    """
    Test extract_primary_entity_descendant_relationship (case 1)

    === Family Tree ===
     grandpa  granny
        \     /
        father    mother
            \     /
              son
    """
    extractor = entity_relationship_extractor
    id_map = {
        # relationship
        "grandpa_father": relationship_grandpa_father.id,
        "granny_father": relationship_granny_father.id,
        "father_son": relationship_father_son.id,
        "mother_son": relationship_mother_son.id,
        # entity
        "grandpa": grandpa_entity_id,
        "granny": granny_entity_id,
        "father": father_entity_id,
        "mother": mother_entity_id,
        "son": son_entity_id,
    }
    primary_entity_ids = [id_map[entity] for entity in entities]
    expected_relationship_ids = set(id_map[relationship] for relationship in expected_relationships)
    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=primary_entity_ids,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == expected_relationship_ids


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entities,expected_relationships",
    [
        (["grandpa"], ["grandpa_father"]),
        (["father"], []),
        (["mother"], ["mother_son"]),
        (["father", "mother"], ["mother_son"]),
    ],
)
async def test_extract_primary_entity_descendant_relationship__case_2(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_mother_son,
    grandpa_entity_id,
    father_entity_id,
    mother_entity_id,
    son_entity_id,
    entities,
    expected_relationships,
):
    """
    Test extract_primary_entity_descendant_relationship (case 2)

    === Family Tree ===
        grandpa
          /
     father  mother
              /
            son
    """
    extractor = entity_relationship_extractor
    id_map = {
        # relationship
        "grandpa_father": relationship_grandpa_father.id,
        "mother_son": relationship_mother_son.id,
        # entity
        "grandpa": grandpa_entity_id,
        "father": father_entity_id,
        "mother": mother_entity_id,
        "son": son_entity_id,
    }
    primary_entity_ids = [id_map[entity] for entity in entities]
    expected_relationship_ids = set(id_map[relationship] for relationship in expected_relationships)
    output = await extractor.extract_primary_entity_descendant_relationship(
        primary_entity_ids=primary_entity_ids,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == expected_relationship_ids


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
