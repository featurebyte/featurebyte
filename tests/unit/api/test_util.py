"""
Tests for api/util.py
"""
import pytest
from bson import ObjectId

from featurebyte.api.entity import Entity
from featurebyte.api.util import get_entity, get_entity_by_id
from featurebyte.exception import RecordRetrievalException


@pytest.fixture(name="entity_id_map")
def entity_id_map_fixture():
    """
    Fixture for a list of entities for testing
    """
    entity_id_map = {}
    for idx in range(10):
        entity_id_map[idx] = Entity.create(name=f"entity_{idx}", serving_name=f"entity_{idx}").id
    yield entity_id_map


def test_event_data_column__get_entity_id(entity_id_map):
    """
    Test get_entity_id given entity name
    """
    # test retrieval
    for idx, expected_entity_id in entity_id_map.items():
        entity = get_entity(entity_name=f"entity_{idx}")
        assert entity["id"] == str(expected_entity_id)

    # test unknown entity
    with pytest.raises(RecordRetrievalException) as exc:
        get_entity(entity_name="unknown_entity")
    assert 'Entity name "unknown_entity" not found!' in str(exc.value)


def test_get_entity_by_id(entity_id_map):
    """
    Test get_entity_by_id
    """
    # test retrieval
    for expected_entity_id in entity_id_map.values():
        entity = get_entity_by_id(str(expected_entity_id))
        assert entity["id"] == str(expected_entity_id)

    # test unknown entity
    with pytest.raises(RecordRetrievalException) as exc:
        unknown_id = str(ObjectId())
        get_entity_by_id(unknown_id)
    assert f'Entity id "{unknown_id}" not found!' in str(exc.value)
