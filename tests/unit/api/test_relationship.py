"""
Test relationships module
"""
import pytest
import pytest_asyncio

from featurebyte import Entity
from featurebyte.api.relationship import Relationship
from featurebyte.models.base import DEFAULT_USER_ID, PydanticObjectId
from featurebyte.models.relationship import RelationshipInfo, RelationshipType
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_service")
def relationship_info_service_fixture(app_container):
    """
    RelationshipInfoService fixture
    """
    return app_container.relationship_info_service


@pytest.fixture(name="persistable_relationship_info")
def persistable_relationship_info_fixture(
    relationship_info_service, snowflake_feature_store, snowflake_event_data
):
    """
    Get a callback function that will persist a relationship info.
    """
    snowflake_feature_store.save()
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()
    user_entity = Entity(name="user", serving_names=["user_id"])
    user_entity.save()
    snowflake_event_data.save()

    async def save() -> RelationshipInfo:
        created_relationship = await relationship_info_service.create_document(
            RelationshipInfoCreate(
                name="test_relationship",
                relationship_type=RelationshipType.CHILD_PARENT,
                primary_entity_id=cust_entity.id,
                related_entity_id=user_entity.id,
                primary_data_source_id=snowflake_event_data.id,
                is_enabled=False,
                updated_by=PydanticObjectId(DEFAULT_USER_ID),
            )
        )
        assert created_relationship.primary_entity_id == cust_entity.id
        return created_relationship

    return save


@pytest_asyncio.fixture(name="persisted_relationship_info")
async def persisted_relationship_info_fixture(persistable_relationship_info):
    """
    Persisted relationship info fixture
    """
    persisted = await persistable_relationship_info()
    yield persisted


def test_accessing_persisted_relationship_info_attributes(persisted_relationship_info):
    """
    Test accessing relationship info attributes
    """
    # Retrieving one copy from the database
    version_1 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_1.is_enabled

    # Retrieving another copy from the database
    version_2 = Relationship.get_by_id(id=persisted_relationship_info.id)
    assert not version_2.is_enabled

    # Update the enabled value on the first in-memory version
    version_1.enable(True)

    # Check that both versions are updated
    assert version_1.is_enabled
    assert version_2.is_enabled


def assert_relationship_info(relationship_info_df):
    """
    Helper function to assert state of relationship_info
    """
    assert relationship_info_df.shape[0] == 1
    assert relationship_info_df["id"][0] is not None
    assert relationship_info_df["primary_entity_name"][0] == "customer"
    assert relationship_info_df["related_entity_name"][0] == "user"
    assert relationship_info_df["data_source_name"][0] == "sf_event_data"
    assert relationship_info_df["updated_by"][0] == "default user"
    assert relationship_info_df["data_type"][0] == "event_data"
    assert not relationship_info_df["is_enabled"][0]


@pytest.mark.asyncio
async def test_relationships_list(persistable_relationship_info):
    """
    Test relationships list
    """
    relationships = Relationship.list()
    assert relationships.shape[0] == 0

    persisted_relationship_info = await persistable_relationship_info()
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
    assert not persisted_relationship_info.is_enabled

    # retrieve relationship via get_by_id
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.is_enabled

    # enable relationship
    relationship.enable(True)

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert relationship.is_enabled

    # disable relationship
    relationship.enable(False)

    # verify that relationship is now enabled
    relationship = Relationship.get_by_id(persisted_relationship_info.id)
    assert not relationship.is_enabled
