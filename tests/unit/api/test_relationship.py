"""
Test relationships module
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import Entity
from featurebyte.api.relationship import Relationship
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.fixture(name="relationship_info_create")
def relationship_info_create_fixture(snowflake_event_table):
    """
    Get a default RelationshipInfoCreate object.
    """
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()
    user_entity = Entity(name="user", serving_names=["user_id"])
    user_entity.save()

    return RelationshipInfoCreate(
        name="test_relationship",
        relationship_type=RelationshipType.CHILD_PARENT,
        entity_id=cust_entity.id,
        related_entity_id=user_entity.id,
        relation_table_id=snowflake_event_table.id,
        enabled=False,
        updated_by=PydanticObjectId(ObjectId()),
    )


@pytest.fixture(name="persistable_relationship_info")
def persistable_relationship_info_fixture(relationship_info_service):
    """
    Get a callback function that will persist a relationship info.
    """

    async def save(relationship_info_create: RelationshipInfoCreate) -> RelationshipInfoModel:
        created_relationship = await relationship_info_service.create_document(
            relationship_info_create
        )
        assert created_relationship.entity_id == relationship_info_create.entity_id
        return created_relationship

    return save


@pytest_asyncio.fixture(name="persisted_relationship_info")
async def persisted_relationship_info_fixture(
    persistable_relationship_info, relationship_info_create
):
    """
    Persisted relationship info fixture
    """
    persisted = await persistable_relationship_info(relationship_info_create)
    yield persisted


@pytest.mark.asyncio
async def test_relationship_get_by_id_without_updated_by(
    persistable_relationship_info, relationship_info_create
):
    """
    Test relationship get by id without updated by field.
    """
    # Create a RelationshipInfoCreate struct with no updated_by field
    default_values = relationship_info_create.dict()
    default_values["updated_by"] = None
    updated_relationship_info_create = RelationshipInfoCreate(**default_values)

    # Persist the value
    persisted_relationship_info = await persistable_relationship_info(
        updated_relationship_info_create
    )
    assert persisted_relationship_info.updated_by is None

    # Test that the results are equal
    retrieved_relationship_info = Relationship.get_by_id(persisted_relationship_info.id)
    assert retrieved_relationship_info.id == persisted_relationship_info.id
    assert retrieved_relationship_info.cached_model == persisted_relationship_info


def test_accessing_persisted_relationship_info_attributes(persisted_relationship_info):
    """
    Test accessing relationship info attributes
    """
    # Retrieving one copy from the database
    version_1 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_1.enabled

    # Retrieving another copy from the database
    version_2 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_2.enabled

    # Update the enabled value on the first in-memory version
    version_1.enable()
    assert version_1.enabled is True

    # Check that both versions are updated
    assert version_1.enabled
    assert version_2.enabled


def assert_relationship_info(relationship_info_df):
    """
    Helper function to assert state of relationship_info
    """
    assert relationship_info_df.shape[0] == 1
    assert relationship_info_df["id"][0] is not None
    assert relationship_info_df["relationship_type"][0] == "child_parent"
    assert relationship_info_df["entity"][0] == "customer"
    assert relationship_info_df["related_entity"][0] == "user"
    assert relationship_info_df["relation_table"][0] == "sf_event_table"
    assert relationship_info_df["relation_table_type"][0] == "event_table"
    assert not relationship_info_df["enabled"][0]


@pytest.mark.asyncio
async def test_relationships_list(persistable_relationship_info, relationship_info_create):
    """
    Test relationships list
    """
    relationships = Relationship.list()
    assert relationships.shape[0] == 0

    persisted_relationship_info = await persistable_relationship_info(relationship_info_create)
    relationship_type = persisted_relationship_info.relationship_type

    # verify that there's one relationship that was created
    relationships = Relationship.list()
    assert_relationship_info(relationships)

    # apply relationship_type filter for existing filter using enum
    relationships = Relationship.list(relationship_type=relationship_type)
    assert_relationship_info(relationships)

    # apply relationship_type filter for existing filter using string
    relationships = Relationship.list(relationship_type="child_parent")
    assert_relationship_info(relationships)

    # apply relationship_type filter for non-existing filter
    with pytest.raises(TypeError):
        Relationship.list(relationship_type="random_filter")


@pytest.mark.asyncio
async def test_enable(persisted_relationship_info):
    """
    Test enable
    """
    # verify that relationship is not enabled
    assert not persisted_relationship_info.enabled

    # retrieve relationship via get_by_id
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.enabled

    # enable relationship
    relationship.enable()

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert relationship.enabled

    # disable relationship
    relationship.disable()
    assert relationship.enabled is False

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.enabled
