"""
Unit test for Entity class
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.exception import DuplicatedRecordException


@pytest.fixture(name="entity")
def entity_fixture(mock_config_path_env, mock_get_persistent):
    """
    Entity fixture
    """
    _ = mock_config_path_env, mock_get_persistent
    yield Entity(name="customer", serving_name="cust_id")


def test_entity_creation(entity):
    """
    Test entity creation
    """
    assert entity.name == "customer"
    assert entity.serving_name == "cust_id"
    assert entity.name_history == []

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="customer", serving_name="customer_id")
    assert exc.value.response.json() == {"detail": 'Entity name "customer" already exists.'}

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="Customer", serving_name="cust_id")
    assert exc.value.response.json() == {"detail": 'Entity serving name "cust_id" already exists.'}


def test_entity_update_name(entity):
    """
    Test update entity name
    """
    assert entity.name_history == []
    entity_id = entity.id
    entity.update_name("Customer")
    assert entity.id == entity_id
    assert entity.name_history == ["customer"]

    # create another entity
    Entity(name="product", serving_name="product_id")

    with pytest.raises(DuplicatedRecordException) as exc:
        entity.update_name("product")
    assert exc.value.response.json() == {"detail": 'Entity name "product" already exists.'}
