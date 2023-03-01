"""
Test relationship_info controller
"""
import json
import os

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import Entity
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipType
from featurebyte.schema.event_data import EventDataCreate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.schema.relationship_info import RelationshipInfoCreate


@pytest.fixture(name="relationship_info_controller")
def relationship_info_controller_fixture(app_container):
    """
    relationship_info_controller fixture
    """
    return app_container.relationship_info_controller


@pytest.fixture(name="relationship_info_create")
def relationship_info_create_fixture():
    """
    relationship_info_create fixture
    """
    primary_entity_id = PydanticObjectId(ObjectId())
    related_entity_id = PydanticObjectId(ObjectId())
    child_data_source_id = PydanticObjectId(ObjectId())
    return RelationshipInfoCreate(
        name="random",
        relationship_type=RelationshipType.CHILD_PARENT,
        primary_entity_id=primary_entity_id,
        related_entity_id=related_entity_id,
        child_data_source_id=child_data_source_id,
        is_enabled=False,
        updated_by=PydanticObjectId(ObjectId()),
    )


@pytest.mark.asyncio
async def test_validate_relationship_info_create__entity_id_error_thrown(
    relationship_info_controller, relationship_info_create
):
    """
    Test validate_relationship_info_create
    """
    with pytest.raises(ValueError) as exc:
        await relationship_info_controller._validate_relationship_info_create(
            relationship_info_create
        )
    assert "entity IDs not found" in str(exc)


@pytest.fixture(name="entities")
def entities_fixture(relationship_info_create):
    """
    Create entities
    """
    entity_1 = Entity(
        name="entity_1", serving_names=["entity_1"], _id=relationship_info_create.primary_entity_id
    )
    entity_1.save()
    entity_2 = Entity(
        name="entity_2", serving_names=["entity_2"], _id=relationship_info_create.related_entity_id
    )
    entity_2.save()


@pytest.mark.asyncio
async def test_validate_relationship_info_create__tabular_data_id_error_thrown(
    relationship_info_controller, relationship_info_create, entities
):
    """
    Test validate_relationship_info_create
    """
    _ = entities

    # Try to create relationship info again - expect a different error from missing data source
    with pytest.raises(DocumentNotFoundError) as exc:
        await relationship_info_controller._validate_relationship_info_create(
            relationship_info_create
        )
    assert "Please save the TabularData object first" in str(exc)


@pytest_asyncio.fixture(name="event_data")
async def event_data_fixture(app_container):
    """
    Create event_data fixture
    """
    fixture_path = os.path.join("tests/fixtures/request_payloads/event_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload["tabular_source"]["table_details"]["table_name"] = "sf_event_table"
        event_data = await app_container.event_data_service.create_document(
            data=EventDataCreate(**payload)
        )
        yield event_data


@pytest_asyncio.fixture(name="feature_store")
async def feature_store_fixture(app_container):
    """FeatureStore model"""
    fixture_path = os.path.join("tests/fixtures/request_payloads/feature_store.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_store = await app_container.feature_store_service.create_document(
            data=FeatureStoreCreate(**payload)
        )
        return feature_store


@pytest.mark.asyncio
async def test_validate_relationship_info_create__no_error_thrown(
    relationship_info_controller, relationship_info_create, entities, feature_store, event_data
):
    """
    Test validate_relationship_info_create
    """
    _, _ = feature_store, entities

    # Try to create relationship info again - expect no error
    create_dict = relationship_info_create.dict()
    create_dict["child_data_source_id"] = event_data.id  # update data source ID to a valid data ID
    relationship_info_create = RelationshipInfoCreate(**create_dict)
    await relationship_info_controller._validate_relationship_info_create(relationship_info_create)
