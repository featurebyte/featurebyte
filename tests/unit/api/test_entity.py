"""
Unit test for Entity class
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.exception import DuplicatedRecordException


def test_entity_creation(mock_config_path_env, mock_get_persistent):
    """
    Test entity creation
    """
    _ = mock_config_path_env, mock_get_persistent
    entity = Entity(name="customer", serving_name="cust_id")
    assert entity.name == "customer"
    assert entity.serving_name == "cust_id"
    assert entity.name_history == []

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="customer", serving_name="customer_id")
    assert exc.value.response.json() == {"detail": 'Entity name "customer" already exists.'}

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="Customer", serving_name="cust_id")
    assert exc.value.response.json() == {"detail": 'Entity serving name "cust_id" already exists.'}
