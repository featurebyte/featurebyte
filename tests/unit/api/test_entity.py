"""
Unit test for Entity class
"""
from datetime import datetime
from unittest import mock

import pytest
from freezegun import freeze_time

from featurebyte.api.entity import Entity
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordUpdateException,
)
from featurebyte.models.entity import EntityNameHistoryEntry


@pytest.fixture(name="entity")
def entity_fixture():
    """
    Entity fixture
    """
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    yield entity


def test_entity_creation__input_validation():
    """
    Test entity creation input validation
    """
    entity = Entity(name="hello", serving_names=["world"])
    with pytest.raises(ValueError) as exc:
        entity.name = 1234
        entity.save()
    assert exc.value.errors() == [
        {"loc": ("name",), "msg": "str type expected", "type": "type_error.str"}
    ]

    entity = Entity(name="hello", serving_names=["world"])
    with pytest.raises(ValueError) as exc:
        entity.serving_names = [1234]
        entity.save()
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
        Entity(name="customer", serving_names=["customer_id"]).save()
    assert 'Entity name (entity.name: "customer") already exists.' in str(exc.value)

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="Customer", serving_names=["cust_id"]).save()
    assert 'Entity serving name (entity.serving_names: "cust_id") already exists.' in str(exc.value)

    with mock.patch("featurebyte.api.entity.Configurations"):
        with pytest.raises(RecordCreationException):
            Entity(name="Customer", serving_names=["cust_id"]).save()


@freeze_time("2022-07-01")
def test_entity_update_name(entity):
    """
    Test update entity name
    """
    assert entity.name_history == []
    entity_id = entity.id
    entity.update_name("Customer")
    assert entity.id == entity_id
    assert entity.name_history == [
        EntityNameHistoryEntry(created_at=datetime(2022, 7, 1), name="customer")
    ]

    # create another entity
    Entity(name="product", serving_names=["product_id"]).save()

    with pytest.raises(ValueError) as exc:
        entity.update_name(type)
    assert exc.value.errors() == [
        {"loc": ("name",), "msg": "str type expected", "type": "type_error.str"}
    ]

    with pytest.raises(DuplicatedRecordException) as exc:
        entity.update_name("product")
    assert exc.value.response.json() == {
        "detail": 'Entity name (entity.name: "product") already exists.'
    }

    with mock.patch("featurebyte.api.entity.Configurations"):
        with pytest.raises(RecordUpdateException):
            entity.update_name("hello")


def test_get_entity():
    """
    Test Entity.get function
    """
    # create entities & save to persistent
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    prod_entity = Entity(name="product", serving_names=["prod_id"])
    region_entity = Entity(name="region", serving_names=["region"])
    cust_entity.save()
    prod_entity.save()
    region_entity.save()

    # load the entities from the persistent
    assert Entity.get("customer") == cust_entity
    assert Entity.get("product") == prod_entity
    assert Entity.get("region") == region_entity
