"""
Test entity relationship extractor
"""
import pytest


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
