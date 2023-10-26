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
async def test_extractor(
    entity_relationship_extractor,
    relationship_grandpa_father,
    relationship_granny_father,
    relationship_father_son,
    relationship_mother_son,
    father_entity_id,
    mother_entity_id,
):
    """Test extractor"""
    extractor = entity_relationship_extractor
    output = await extractor.extract(
        entity_ids=[father_entity_id],
        keep_all_ancestors=True,
        keep_all_descendants=True,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {
        relationship_grandpa_father.id,
        relationship_granny_father.id,
        relationship_father_son.id,
    }

    output = await extractor.extract(
        entity_ids=[mother_entity_id],
        keep_all_ancestors=True,
        keep_all_descendants=True,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_mother_son.id}

    output = await extractor.extract(
        entity_ids=[father_entity_id],
        keep_all_ancestors=False,
        keep_all_descendants=True,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_father_son.id}

    output = await extractor.extract(
        entity_ids=[father_entity_id],
        keep_all_ancestors=True,
        keep_all_descendants=False,
    )
    relationship_ids = set(relationship.id for relationship in output)
    assert relationship_ids == {relationship_grandpa_father.id, relationship_granny_father.id}

    output = await extractor.extract(
        entity_ids=[father_entity_id],
        keep_all_ancestors=False,
        keep_all_descendants=False,
    )
    assert output == []
