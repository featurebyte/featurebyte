"""
Unit test for Entity class
"""
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from featurebyte.api.entity import Entity
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)


@pytest.fixture(name="entity")
def entity_fixture():
    """
    Entity fixture
    """
    entity = Entity(name="customer", serving_names=["cust_id"])
    previous_id = entity.id
    assert entity.saved is False
    entity.save()
    assert entity.saved is True
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
    assert entity.saved is True

    # test update name (non-saved object)
    another_entity = Entity(name="AnotherCustomer", serving_names=["cust"])
    with pytest.raises(RecordRetrievalException) as exc:
        lazy_entity = Entity.get("AnotherCustomer")
        _ = lazy_entity.name
    expected_msg = (
        'Entity (name: "AnotherCustomer") not found. ' "Please save the Entity object first."
    )
    assert expected_msg in str(exc.value)
    assert another_entity.name == "AnotherCustomer"
    another_entity.update_name("another_customer")
    assert another_entity.name == "another_customer"
    assert another_entity.saved is False


def test_info(entity):
    """
    Test info
    """
    info_dict = entity.info(verbose=True)
    expected_info = {
        "name": "customer",
        "serving_names": ["cust_id"],
        "updated_at": None,
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


def test_entity_creation(entity):
    """
    Test entity creation
    """
    assert entity.name == "customer"
    assert entity.serving_name == "cust_id"
    name_history = entity.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "customer"}.items()

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


def test_entity_update_name(entity):
    """
    Test update entity name
    """
    name_history = entity.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "customer"}.items()

    entity_id = entity.id
    tic = datetime.utcnow()
    entity.update_name("Customer")
    toc = datetime.utcnow()
    assert entity.id == entity_id
    name_history = entity.name_history
    assert len(name_history) == 2
    assert name_history[0].items() >= {"name": "Customer"}.items()
    assert toc > datetime.fromisoformat(name_history[0]["created_at"]) > tic
    assert name_history[1].items() >= {"name": "customer"}.items()
    assert tic > datetime.fromisoformat(name_history[1]["created_at"])

    # check audit history
    audit_history = entity.audit()
    expected_paginatation_info = {"page": 1, "page_size": 10, "total": 2}
    assert audit_history.items() >= expected_paginatation_info.items()
    history_data = audit_history["data"]
    assert len(history_data) == 2
    assert (
        history_data[0].items()
        > {
            "name": 'update: "customer"',
            "action_type": "UPDATE",
            "previous_values": {"name": "customer", "updated_at": None},
        }.items()
    )
    assert history_data[0]["current_values"].items() > {"name": "Customer"}.items()
    assert (
        history_data[1].items()
        > {"name": 'insert: "customer"', "action_type": "INSERT", "previous_values": {}}.items()
    )
    assert (
        history_data[1]["current_values"].items()
        > {"name": "customer", "updated_at": None, "serving_names": ["cust_id"]}.items()
    )

    # create another entity
    Entity(name="product", serving_names=["product_id"]).save()

    with pytest.raises(TypeError) as exc:
        entity.update_name(type)
    assert 'type of argument "name" must be str; got type instead' in str(exc.value)

    with pytest.raises(DuplicatedRecordException) as exc:
        entity.update_name("product")
    assert exc.value.response.json() == {
        "detail": (
            'Entity (name: "product") already exists. '
            'Get the existing object by `Entity.get(name="product")`.'
        )
    }

    with mock.patch("featurebyte.api.api_object.Configurations"):
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
    get_cust_entity = Entity.get("customer")
    assert get_cust_entity.saved is True
    assert get_cust_entity.dict(exclude=exclude) == cust_entity.dict(exclude=exclude)
    assert Entity.get("product").dict(exclude=exclude) == prod_entity.dict(exclude=exclude)
    assert Entity.get("region").dict(exclude=exclude) == region_entity.dict(exclude=exclude)
    assert Entity.get_by_id(id=cust_entity.id) == cust_entity

    # test unexpected retrieval exception for Entity.get
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            lazy_entity = Entity.get("anything")
            _ = lazy_entity.name
    assert "Failed to retrieve the specified object." in str(exc.value)

    # test list entity names
    entity_list = Entity.list()
    assert_frame_equal(
        entity_list,
        pd.DataFrame(
            {
                "name": [region_entity.name, prod_entity.name, cust_entity.name],
                "serving_names": [
                    region_entity.serving_names,
                    prod_entity.serving_names,
                    cust_entity.serving_names,
                ],
                "created_at": [
                    region_entity.created_at,
                    prod_entity.created_at,
                    cust_entity.created_at,
                ],
            }
        ),
    )

    # test unexpected retrieval exception for Entity.list
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Entity.list()
    assert "Failed to list object names." in str(exc.value)
