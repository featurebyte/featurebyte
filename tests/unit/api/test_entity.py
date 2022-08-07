"""
Unit test for Entity class
"""
import json
from datetime import datetime
from unittest import mock

import pytest
from freezegun import freeze_time
from pydantic import ValidationError

from featurebyte.api.entity import Entity
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.entity import EntityNameHistoryEntry


@pytest.fixture(name="entity")
def entity_fixture():
    """
    Entity fixture
    """
    entity = Entity(name="customer", serving_names=["cust_id"])
    previous_id = entity.id
    entity.save()
    assert entity.id == previous_id
    yield entity


def test_entity_creation__input_validation():
    """
    Test entity creation input validation
    """
    entity = Entity(name="hello", serving_names=["world"])
    with pytest.raises(ValidationError) as exc:
        entity.name = 1234
    assert "str type expected (type=type_error.str)" in str(exc.value)

    entity = Entity(name="hello", serving_names=["world"])
    with pytest.raises(TypeError) as exc:
        entity.serving_names = ["1234"]
    assert '"serving_names" has allow_mutation set to False and cannot be assigned' in str(
        exc.value
    )


def test_entity__update_name(entity):
    """
    Test update_name in Entity class
    """
    # test update name (saved object)
    assert entity.name == "customer"
    entity.update_name("Customer")
    assert entity.name == "Customer"

    # test update name (non-saved object)
    another_entity = Entity(name="AnotherCustomer", serving_names=["cust"])
    with pytest.raises(RecordRetrievalException) as exc:
        Entity.get("AnotherCustomer")
    expected_msg = (
        'Entity (name: "AnotherCustomer") not found. ' "Please save the Entity object first."
    )
    assert expected_msg in str(exc.value)
    assert another_entity.name == "AnotherCustomer"
    another_entity.update_name("another_customer")
    assert another_entity.name == "another_customer"


def test_entity_creation(entity):
    """
    Test entity creation
    """
    assert entity.name == "customer"
    assert entity.serving_name == "cust_id"
    assert entity.name_history == []

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="customer", serving_names=["customer_id"]).save()
    expected_msg = (
        'Entity (name: "customer") already exists. '
        'Get the existing object by `Entity.get(name="customer")`.'
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(DuplicatedRecordException) as exc:
        Entity(name="Customer", serving_names=["cust_id"]).save()
    expected_msg = (
        'Entity (serving_name: "cust_id") already exists. '
        'Get the existing object by `Entity.get(name="customer")`.'
    )
    assert expected_msg in str(exc.value)

    with mock.patch("featurebyte.api.api_object.Configurations"):
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
        "detail": (
            'Entity (name: "product") already exists. '
            'Get the existing object by `Entity.get(name="product")`.'
        )
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
    exclude = {"created_at": True, "updated_at": True}
    assert Entity.get("customer").dict(exclude=exclude) == cust_entity.dict(exclude=exclude)
    assert Entity.get("product").dict(exclude=exclude) == prod_entity.dict(exclude=exclude)
    assert Entity.get("region").dict(exclude=exclude) == region_entity.dict(exclude=exclude)
    assert Entity.get_by_id(id=cust_entity.id) == cust_entity

    # test unexpected retrieval exception for Entity.get
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Entity.get("anything")
    assert "Failed to retrieve the specified object." in str(exc.value)

    # test list entity names
    assert Entity.list() == ["region", "product", "customer"]

    # test unexpected retrieval exception for Entity.list
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Entity.list()
    assert "Failed to list object names." in str(exc.value)
