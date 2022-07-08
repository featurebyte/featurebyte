"""
Unit test for Entity class
"""
from unittest import mock

import pytest

from featurebyte.api.entity import Entity
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordUpdateException,
)


@pytest.fixture(name="entity")
def entity_fixture(mock_config_path_env, mock_get_persistent):
    """
    Entity fixture
    """
    _ = mock_config_path_env, mock_get_persistent
    yield Entity(name="customer", serving_name="cust_id")


def test_entity_creation__input_validation():
    """
    Test entity creation input validation
    """
    with pytest.raises(ValueError) as exc:
        Entity(name=1234, serving_name="hello")
    assert exc.value.errors() == [
        {"loc": ("name",), "msg": "str type expected", "type": "type_error.str"}
    ]

    with pytest.raises(ValueError) as exc:
        Entity(name="world", serving_name=234)
    assert exc.value.errors() == [
        {"loc": ("serving_name",), "msg": "str type expected", "type": "type_error.str"}
    ]


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

    with mock.patch("featurebyte.api.entity.Configurations"):
        with pytest.raises(RecordCreationException):
            Entity(name="Customer", serving_name="cust_id")


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

    with pytest.raises(ValueError) as exc:
        entity.update_name(type)
    assert exc.value.errors() == [
        {"loc": ("name",), "msg": "str type expected", "type": "type_error.str"}
    ]

    with pytest.raises(DuplicatedRecordException) as exc:
        entity.update_name("product")
    assert exc.value.response.json() == {"detail": 'Entity name "product" already exists.'}

    with mock.patch("featurebyte.api.entity.Configurations"):
        with pytest.raises(RecordUpdateException):
            entity.update_name("hello")
