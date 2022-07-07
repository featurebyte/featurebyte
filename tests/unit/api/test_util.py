"""
Tests for api/util.py
"""
from featurebyte.api.entity import Entity
from featurebyte.api.util import get_entity


def test_event_data_column__get_entity_id(mock_get_persistent):
    """
    Test get_entity_id given entity name
    """
    _ = mock_get_persistent

    # create a list of entity for testing
    entity_id_map = {}
    for idx in range(10):
        entity_id_map[idx] = Entity(name=f"entity_{idx}", serving_name=f"entity_{idx}").id

    # test retrieval
    for page_size in [3, 5]:
        for idx, expected_entity_id in entity_id_map.items():
            entity = get_entity(entity_name=f"entity_{idx}", page_size=page_size)
            assert entity["id"] == str(expected_entity_id)

    # test unknown entity
    assert get_entity(entity_name="unknown_entity") is None
