"""
Test relationships module
"""
import pytest
from bson import ObjectId

from featurebyte.api.relationships import Relationships
from featurebyte.models.base import PydanticObjectId
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
    relationships = Relationships.list()
    assert relationships.shape[0] == 0

    child_id = PydanticObjectId(ObjectId())
    parent_id = PydanticObjectId(ObjectId())
    child_data_source_id = PydanticObjectId(ObjectId())
    updated_by_user_id = PydanticObjectId(ObjectId())

    # create new relationship
    relationship_type = "parent_child"
    created_relationship = await relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            child_id=child_id,
            parent_id=parent_id,
            child_data_source_id=child_data_source_id,
            is_enabled=True,
            updated_by=updated_by_user_id,
        )
    )
    assert created_relationship.child_id == child_id

    # verify that there's one relationship that was created
    relationships = Relationships.list()
    assert relationships.shape[0] == 1
    assert relationships["child_id"][0] == child_id

    # apply relationship_type filter for existing filter
    relationships = Relationships.list(relationship_type=relationship_type)
    assert relationships.shape[0] == 1
    assert relationships["child_id"][0] == child_id

    # apply relationship_type filter for non-existing filter
    relationships = Relationships.list(relationship_type="random_filter")
    assert relationships.shape[0] == 0


@pytest.mark.asyncio
async def test_enable(relationship_info_service):
    """
    Test enable
    """
    child_id = PydanticObjectId(ObjectId())
    parent_id = PydanticObjectId(ObjectId())
    child_data_source_id = PydanticObjectId(ObjectId())
    updated_by_user_id = PydanticObjectId(ObjectId())

    # create new relationship
    relationship_type = "parent_child"
    created_relationship = await relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            child_id=child_id,
            parent_id=parent_id,
            child_data_source_id=child_data_source_id,
            is_enabled=False,
            updated_by=updated_by_user_id,
        )
    )
    assert not created_relationship.is_enabled

    # retrieve relationship via get_by_id
    relationship = Relationships.get_by_id(created_relationship.id)
    assert not relationship.is_enabled

    # enable relationship
    relationship.enable(True)

    # verify that relationship is now enabled
    relationship = Relationships.get_by_id(created_relationship.id)
    assert relationship.is_enabled
