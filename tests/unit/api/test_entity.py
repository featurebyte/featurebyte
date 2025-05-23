"""
Unit test for Entity class
"""

from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from pandas.testing import assert_frame_equal
from typeguard import TypeCheckError

from featurebyte.api.catalog import Catalog
from featurebyte.api.entity import Entity
from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


@pytest.fixture(name="entity")
def entity_fixture(catalog):
    """
    Entity fixture
    """
    _ = catalog
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
    with pytest.raises(ValueError) as exc:
        entity.name = 1234
    assert "Input should be a valid string" in str(exc.value)


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
        Entity.get("AnotherCustomer")

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

    with mock.patch("featurebyte.api.savable_api_object.Configurations"):
        with pytest.raises(RecordCreationException):
            Entity(name="Customer", serving_names=["cust_id"]).save()


def test_entity_update_name(entity, catalog, user_id):
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
    expected_audit_history = pd.DataFrame(
        [
            ("UPDATE", 'update: "customer"', "name", "customer", "Customer"),
            ("UPDATE", 'update: "customer"', "updated_at", None, entity.updated_at.isoformat()),
            ("INSERT", 'insert: "customer"', "ancestor_ids", np.nan, []),
            ("INSERT", 'insert: "customer"', "block_modification_by", np.nan, []),
            ("INSERT", 'insert: "customer"', "catalog_id", np.nan, str(catalog.id)),
            ("INSERT", 'insert: "customer"', "created_at", np.nan, entity.created_at.isoformat()),
            ("INSERT", 'insert: "customer"', "description", np.nan, None),
            ("INSERT", 'insert: "customer"', "dtype", np.nan, None),
            ("INSERT", 'insert: "customer"', "is_deleted", np.nan, False),
            ("INSERT", 'insert: "customer"', "name", np.nan, "customer"),
            ("INSERT", 'insert: "customer"', "parents", np.nan, []),
            ("INSERT", 'insert: "customer"', "primary_table_ids", np.nan, []),
            ("INSERT", 'insert: "customer"', "serving_names", np.nan, ["cust_id"]),
            ("INSERT", 'insert: "customer"', "table_ids", np.nan, []),
            ("INSERT", 'insert: "customer"', "updated_at", np.nan, None),
            ("INSERT", 'insert: "customer"', "user_id", np.nan, str(user_id)),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )

    # create another entity
    Entity(name="product", serving_names=["product_id"]).save()

    with pytest.raises(TypeCheckError) as exc:
        entity.update_name(type)
    assert 'argument "name" (class type) is not an instance of str' in str(exc.value)

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


def test_get_entity(catalog):
    """
    Test Entity.get function
    """
    _ = catalog

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
    assert get_cust_entity.model_dump(exclude=exclude) == cust_entity.model_dump(exclude=exclude)
    assert Entity.get("product").model_dump(exclude=exclude) == prod_entity.model_dump(
        exclude=exclude
    )
    assert Entity.get("region").model_dump(exclude=exclude) == region_entity.model_dump(
        exclude=exclude
    )
    assert Entity.get_by_id(id=cust_entity.id) == cust_entity

    # test unexpected retrieval exception for Entity.get
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Entity.get("anything")

    assert "Failed to retrieve the specified object." in str(exc.value)

    # test list entity names - no include_id
    entity_list = Entity.list(include_id=False)
    expected_entity_list = pd.DataFrame({
        "name": [region_entity.name, prod_entity.name, cust_entity.name],
        "serving_names": [
            region_entity.serving_names,
            prod_entity.serving_names,
            cust_entity.serving_names,
        ],
        "created_at": [
            region_entity.created_at.isoformat(),
            prod_entity.created_at.isoformat(),
            cust_entity.created_at.isoformat(),
        ],
    })
    assert_frame_equal(entity_list, expected_entity_list)

    # test list with include_id=True
    entity_list = Entity.list()
    expected_entity_list["id"] = [str(region_entity.id), str(prod_entity.id), str(cust_entity.id)]
    assert_frame_equal(entity_list, expected_entity_list[entity_list.columns])

    # test unexpected retrieval exception for Entity.list
    with mock.patch("featurebyte.api.api_object_util.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Entity.list()
    assert "Failed to list /entity." in str(exc.value)


@pytest.fixture(name="insert_table_helper")
def get_insert_table_helper_fixture(mongo_persistent):
    persistent, _ = mongo_persistent

    async def insert(col_name, dataset_name):
        test_document = {
            "_id": ObjectId(),
            "name": dataset_name,
            "type": TableDataType.DIMENSION_TABLE,
            "tabular_source": TabularSource(
                feature_store_id=PydanticObjectId(ObjectId()),
                table_details=TableDetails(table_name="test_table"),
            ).json_dict(),
            "columns_info": [ColumnInfo(name=col_name, dtype=DBVarType.INT).json_dict()],
            "dimension_id_column": col_name,
            "version": {"name": "name_val", "suffix": None},
            "catalog_id": DEFAULT_CATALOG_ID,
        }
        user_id = ObjectId()
        _ = await persistent.insert_one(
            collection_name="table", document=test_document, user_id=user_id
        )

    return insert


def test_create(catalog):
    """
    Test Entity.create
    """
    _ = catalog

    entity_name = "random_entity"
    # Verify entity doesn't exist first
    with pytest.raises(RecordRetrievalException) as exc:
        Entity.get(entity_name)
    assert "Please save the Entity object first." in str(exc)

    # Create entity
    entity = Entity.create(entity_name, serving_names=["random_entity_serving_name"])
    assert entity.name == entity_name

    # Test that entity exists
    entity_retrieved = Entity.get(entity_name)
    assert entity_retrieved.id == entity.id
    assert entity_retrieved.name == entity.name


def test_get_or_create(catalog):
    """
    Test get_or_create
    """
    _ = catalog

    entity_name = "random_entity"
    # Verify entity doesn't exist first
    with pytest.raises(RecordRetrievalException) as exc:
        Entity.get(entity_name)
    assert "Please save the Entity object first." in str(exc)

    # Create entity with get_or_create
    entity = Entity.get_or_create(entity_name, serving_names=["random_entity_serving_name"])
    assert entity.name == entity_name

    # Test that entity exists after calling get_or_create once
    entity_retrieved = Entity.get(entity_name)
    assert entity_retrieved.id == entity.id
    assert entity_retrieved.name == entity.name

    # Call get_or_create again - verify that there's no error and entity is retrieved.
    # Also show that if we're just doing the `get` in get or create, the serving_names passed in is irrelevant.
    entity_retrieved = Entity.get_or_create(entity_name, serving_names=[])
    assert entity_retrieved.id == entity.id
    assert entity_retrieved.name == entity.name
    assert entity_retrieved.serving_names == ["random_entity_serving_name"]


def test_entity_name_synchronization_issue(entity):
    """Test entity name synchronization issue."""
    cloned_entity = Entity.get_by_id(entity.id)
    assert entity.name == "customer"
    assert cloned_entity.name == "customer"
    entity.update_name("grocery_customer")
    assert entity.name == "grocery_customer"
    assert cloned_entity.name == "grocery_customer"

    # check it back
    cloned_entity.update_name("customer")
    assert entity.name == "customer"
    assert cloned_entity.name == "customer"


def test_catalog_id(entity):
    """
    Test catalog_id
    """
    entity = Entity.get_by_id(entity.id)
    catalog = Catalog.get_active()
    assert entity.catalog_id == catalog.id


def test_delete(entity):
    """Test delete entity"""
    assert entity.saved is True
    entity.delete()
    assert entity.saved is False
