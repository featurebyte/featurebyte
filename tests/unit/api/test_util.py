"""
Tests for api/util.py
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.util import get_entity
from featurebyte.exception import RecordRetrievalException


def test_event_data_column__get_entity_id(mock_config_path_env, mock_get_persistent):
    """
    Test get_entity_id given entity name
    """
    _ = mock_config_path_env, mock_get_persistent

    # create a list of entity for testing
    entity_id_map = {}
    for idx in range(10):
        entity_id_map[idx] = Entity.create(name=f"entity_{idx}", serving_name=f"entity_{idx}").id

    # test retrieval
    for idx, expected_entity_id in entity_id_map.items():
        entity = get_entity(entity_name=f"entity_{idx}")
        assert entity["id"] == str(expected_entity_id)

    # test unknown entity
    with pytest.raises(RecordRetrievalException) as exc:
        get_entity(entity_name="unknown_entity")
    assert 'Entity name "unknown_entity" not found!' in str(exc.value)
