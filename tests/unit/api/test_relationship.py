"""
Test relationships module
"""
import pytest
from bson import ObjectId

from featurebyte.api.relationship import Relationship
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.mark.asyncio
async def test_relationships_list(relationship_info_service):
    """
    Test relationships list
    """
    relationships = Relationship.list()
    assert relationships.shape[0] == 0

    primary_entity_id = PydanticObjectId(ObjectId())
    related_entity_id = PydanticObjectId(ObjectId())
    primary_data_source_id = PydanticObjectId(ObjectId())
    updated_by_user_id = PydanticObjectId(ObjectId())

    # create new relationship
    relationship_type = RelationshipType.CHILD_PARENT
    created_relationship = await relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            primary_entity_id=primary_entity_id,
            related_entity_id=related_entity_id,
            primary_data_source_id=primary_data_source_id,
            is_enabled=True,
            updated_by=updated_by_user_id,
        )
    )
    assert created_relationship.primary_entity_id == primary_entity_id

    # verify that there's one relationship that was created
    relationships = Relationship.list()
    assert relationships.shape[0] == 1
    assert relationships["primary_entity_id"][0] == primary_entity_id

    # apply relationship_type filter for existing filter using enum
    relationships = Relationship.list(relationship_type=relationship_type)
    assert relationships.shape[0] == 1
    assert relationships["primary_entity_id"][0] == primary_entity_id

    # apply relationship_type filter for existing filter using string
    relationships = Relationship.list(relationship_type="child_parent")
    assert relationships.shape[0] == 1
    assert relationships["primary_entity_id"][0] == primary_entity_id

    # apply relationship_type filter for non-existing filter
    with pytest.raises(TypeError):
        Relationship.list(relationship_type="random_filter")


@pytest.mark.asyncio
async def test_enable(relationship_info_service):
    """
    Test enable
    """
    primary_entity_id = PydanticObjectId(ObjectId())
    related_entity_id = PydanticObjectId(ObjectId())
    primary_data_source_id = PydanticObjectId(ObjectId())
    updated_by_user_id = PydanticObjectId(ObjectId())

    # create new relationship
    relationship_type = RelationshipType.CHILD_PARENT
    created_relationship = await relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            primary_entity_id=primary_entity_id,
            related_entity_id=related_entity_id,
            primary_data_source_id=primary_data_source_id,
            is_enabled=False,
            updated_by=updated_by_user_id,
        )
    )
    assert not created_relationship.is_enabled

    # retrieve relationship via get_by_id
    relationship = Relationship.get_by_id(created_relationship.id)
    assert not relationship.is_enabled

    # enable relationship
    relationship.enable(True)

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(created_relationship.id)
    assert relationship.is_enabled

    # disable relationship
    relationship.enable(False)

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(created_relationship.id)
    assert not relationship.is_enabled
