"""
Unit test for Catalog class
"""
from __future__ import annotations

from typing import Any

from dataclasses import dataclass
from datetime import datetime
from inspect import signature
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from featurebyte import (
    DimensionTable,
    EventTable,
    EventView,
    Feature,
    FeatureJobSettingAnalysis,
    FeatureList,
    FeatureStore,
    ItemTable,
    PeriodicTask,
    Relationship,
    SCDTable,
    Table,
)
from featurebyte.api.api_object import ApiObject, SavableApiObject
from featurebyte.api.base_table import TableApiObject, TableListMixin
from featurebyte.api.catalog import Catalog, update_and_reset_catalog
from featurebyte.api.entity import Entity
from featurebyte.api.feature import FeatureNamespace
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_list import FeatureListNamespace
from featurebyte.config import activate_catalog, get_active_catalog_id
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.base import DEFAULT_CATALOG_ID


@pytest.fixture(autouse=True)
def reset_catalog():
    """
    Reset back to default catalog after every test.
    """
    yield

    activate_catalog(DEFAULT_CATALOG_ID)


@dataclass
class MethodMetadata:
    """
    Data class to keep track of list methods to test.
    """

    # Catalog method to test
    catalog_method_name: str
    # API object which has the list method we delegate to
    class_object: Any
    # Method we delegate to (eg. list/get)
    class_method_delegated: str


def catalog_list_methods_to_test_list():
    return [
        MethodMetadata("list_features", Feature, "list"),
        MethodMetadata("list_feature_lists", FeatureList, "list"),
        MethodMetadata("list_tables", Table, "list"),
        MethodMetadata("list_relationships", Relationship, "list"),
        MethodMetadata("list_feature_job_setting_analyses", FeatureJobSettingAnalysis, "list"),
        MethodMetadata("list_feature_stores", FeatureStore, "list"),
        MethodMetadata("list_entities", Entity, "list"),
        MethodMetadata("list_periodic_tasks", PeriodicTask, "list"),
    ]


def catalog_get_methods_to_test_list():
    return [
        MethodMetadata("get_feature", Feature, "get"),
        MethodMetadata("get_feature_list", FeatureList, "get"),
        MethodMetadata("get_table", Table, "get"),
        MethodMetadata("get_relationship", Relationship, "get"),
        MethodMetadata("get_feature_job_setting_analysis", FeatureJobSettingAnalysis, "get"),
        MethodMetadata("get_feature_store", FeatureStore, "get"),
        MethodMetadata("get_entity", Entity, "get"),
        MethodMetadata("get_periodic_task", PeriodicTask, "get"),
    ]


def catalog_create_methods_to_test_list():
    return [
        MethodMetadata("create_entity", Entity, "create"),
    ]


def test_all_relevant_methods_are_in_list():
    """
    Test that all the relevant get and list methods in the catalog class are in these lists above.
    """
    methods = dir(Catalog)
    # Verify all list methods are present
    list_methods = {method for method in methods if method.startswith("list_")}
    assert len(list_methods) == len(catalog_list_methods_to_test_list())
    for method in catalog_list_methods_to_test_list():
        assert method.catalog_method_name in list_methods

    # Verify all relevant get methods are present
    get_methods = {
        method for method in methods if method.startswith("get_") and not method.endswith("by_id")
    }
    excluded_methods = {
        "get_by_id",
        "get_active",
        "get_or_create",
        "get_data_source",
        "get_view",
        "get_data_source_by_feature_store_id",
    }
    assert len(get_methods) - len(excluded_methods) == len(catalog_get_methods_to_test_list())
    for method in catalog_get_methods_to_test_list():
        assert method.catalog_method_name in get_methods

    # Verify all relevant create methods are present
    create_methods = {method for method in methods if method.startswith("create_")}
    assert len(create_methods) == len(catalog_create_methods_to_test_list())
    for method in catalog_create_methods_to_test_list():
        assert method.catalog_method_name in create_methods


def test_get_data_source(snowflake_feature_store):
    """
    Test that get_data_source returns the correct data source.
    """
    catalog = Catalog.get_active()
    data_source = catalog.get_data_source(snowflake_feature_store.name)
    assert data_source.type == "snowflake"


def test_get_view(snowflake_event_table):
    """
    Test that get_view returns the right view
    """
    catalog = Catalog.get_active()
    view = catalog.get_view(snowflake_event_table.name)
    assert type(view) == EventView


def catalog_get_list_and_create_methods():
    return [
        *catalog_list_methods_to_test_list(),
        *catalog_get_methods_to_test_list(),
        *catalog_create_methods_to_test_list(),
    ]


def _inheritors(class_obj):
    """
    Helper method to find all children of a class.
    """
    subclasses = set()
    work = [class_obj]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


@pytest.mark.parametrize(
    "method_list", [catalog_list_methods_to_test_list(), catalog_get_methods_to_test_list()]
)
def test_all_methods_are_exposed_in_catalog(method_list):
    """
    Test that all inherited list methods are exposed in catalog.

    This will help to ensure that new API objects that have a list method are added to the Catalog.
    If we don't want to add them, we can add them to the excluded_children set.
    """
    api_object_children = _inheritors(ApiObject)
    excluded_children = {
        Catalog,  # accessible as part of Catalog.get
        DimensionTable,  # accessible as part of catalog.(list|get)_table
        EventTable,  # accessible as part of catalog.(list|get)_table
        FeatureJobMixin,
        FeatureListNamespace,  # same as (list|get)_feature_list
        FeatureNamespace,  # same as (list|get)_feature
        ItemTable,  # accessible as part of catalog.(list|get)_table
        SCDTable,  # accessible as part of catalog.(list|get)_table
        SavableApiObject,
        TableApiObject,
        TableListMixin,
    }
    assert len(api_object_children) == len(method_list) + len(excluded_children)

    for method_item in method_list:
        delegated_class = method_item.class_object
        assert delegated_class in api_object_children


@pytest.mark.parametrize(
    "method_item",
    catalog_get_list_and_create_methods(),
)
def test_methods_have_same_parameters_as_delegated_method_call(method_item):
    """
    Test catalog methods have same parameters as underlying methods.
    This will help to ensure that the Catalog APIs are consistent with their delegated API object methods.
    """
    catalog = Catalog.get_active()
    catalog_list_method_name, underlying_class = (
        method_item.catalog_method_name,
        method_item.class_object,
    )
    # Check that the signatures match
    catalog_method = getattr(catalog, catalog_list_method_name)
    catalog_list_method_signature = signature(catalog_method)
    underlying_class_method = getattr(underlying_class, method_item.class_method_delegated)
    underlying_class_list_method_signature = signature(underlying_class_method)
    assert [*catalog_list_method_signature.parameters.keys()] == [
        *underlying_class_list_method_signature.parameters.keys(),
    ], f"catalog method: {catalog_list_method_name}, underlying_class {underlying_class}"


def _invoke_method(catalog: Catalog, method_item: MethodMetadata):
    """
    Helper method to invoke the catalog method of the method item.
    """
    catalog_method_to_call = getattr(catalog, method_item.catalog_method_name)
    if method_item.class_method_delegated == "get":
        catalog_method_to_call("random_name")
    elif method_item.class_method_delegated == "create":
        catalog_method_to_call("random_name", [])
    else:
        catalog_method_to_call()


@pytest.mark.parametrize(
    "method_item",
    catalog_get_list_and_create_methods(),
)
def test_methods_call_the_correct_delegated_method(method_item):
    """
    Test catalog methods call the correct delegated method.
    """
    # Assert that the delegated list method is called
    method_name = method_item.class_method_delegated
    catalog = Catalog.get_active()
    with patch.object(method_item.class_object, method_name) as mocked_list:
        _invoke_method(catalog, method_item)
        mocked_list.assert_called()


@pytest.fixture(name="catalog")
def catalog_fixture():
    """
    Catalog fixture
    """
    catalog = Catalog(name="grocery")
    previous_id = catalog.id
    assert catalog.saved is False
    catalog.save()
    assert catalog.saved is True
    assert catalog.id == previous_id
    yield catalog


def test_catalog_creation__input_validation():
    """
    Test catalog creation input validation
    """
    with pytest.raises(ValidationError) as exc:
        Catalog(name=123)
    assert "str type expected (type=type_error.str)" in str(exc.value)


def test_catalog__update_name(catalog):
    """
    Test update_name in Catalog class
    """
    # test update name (saved object)
    assert catalog.name == "grocery"
    catalog.update_name("Grocery")
    assert catalog.name == "Grocery"
    assert catalog.saved is True

    # test update name (non-saved object)
    another_catalog = Catalog(name="CreditCard")
    with pytest.raises(RecordRetrievalException) as exc:
        Catalog.get("CreditCard")
    expected_msg = (
        'Catalog (name: "CreditCard") not found. ' "Please save the Catalog object first."
    )
    assert expected_msg in str(exc.value)
    assert another_catalog.name == "CreditCard"
    another_catalog.update_name("creditcard")
    assert another_catalog.name == "creditcard"
    assert another_catalog.saved is False


def test_info(catalog):
    """
    Test info
    """
    info_dict = catalog.info(verbose=True)
    expected_info = {
        "name": "grocery",
        "updated_at": None,
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


def test_catalog_creation(catalog):
    """
    Test catalog creation
    """
    assert catalog.name == "grocery"
    name_history = catalog.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "grocery"}.items()

    with pytest.raises(DuplicatedRecordException) as exc:
        Catalog(name="grocery").save()
    expected_msg = (
        'Catalog (name: "grocery") already exists. '
        'Get the existing object by `Catalog.get(name="grocery")`.'
    )
    assert expected_msg in str(exc.value)


def test_catalog_update_name(catalog):
    """
    Test update catalog name
    """
    name_history = catalog.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "grocery"}.items()

    catalog_id = catalog.id
    tic = datetime.utcnow()
    catalog.update_name("Grocery")
    toc = datetime.utcnow()
    assert catalog.id == catalog_id
    name_history = catalog.name_history
    assert len(name_history) == 2
    assert name_history[0].items() >= {"name": "Grocery"}.items()
    assert toc > datetime.fromisoformat(name_history[0]["created_at"]) > tic
    assert name_history[1].items() >= {"name": "grocery"}.items()
    assert tic > datetime.fromisoformat(name_history[1]["created_at"])

    # check audit history
    audit_history = catalog.audit()
    expected_audit_history = pd.DataFrame(
        [
            ("UPDATE", 'update: "grocery"', "name", "grocery", "Grocery"),
            ("UPDATE", 'update: "grocery"', "updated_at", None, catalog.updated_at.isoformat()),
            ("INSERT", 'insert: "grocery"', "created_at", np.nan, catalog.created_at.isoformat()),
            ("INSERT", 'insert: "grocery"', "name", np.nan, "grocery"),
            ("INSERT", 'insert: "grocery"', "updated_at", np.nan, None),
            ("INSERT", 'insert: "grocery"', "user_id", np.nan, None),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )

    # create another catalog
    Catalog(name="creditcard").save()

    with pytest.raises(TypeError) as exc:
        catalog.update_name(type)
    assert 'type of argument "name" must be str; got type instead' in str(exc.value)

    with pytest.raises(DuplicatedRecordException) as exc:
        catalog.update_name("creditcard")
    assert exc.value.response.json() == {
        "detail": (
            'Catalog (name: "creditcard") already exists. '
            'Get the existing object by `Catalog.get(name="creditcard")`.'
        )
    }

    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordUpdateException):
            catalog.update_name("hello")


def test_get_catalog():
    """
    Test Catalog.get function
    """
    # create catalogs & save to persistent
    grocery_catalog = Catalog(name="grocery")
    creditcard_catalog = Catalog(name="creditcard")
    healthcare_catalog = Catalog(name="healthcare")
    grocery_catalog.save()
    creditcard_catalog.save()
    healthcare_catalog.save()

    # load the catalogs from the persistent
    exclude = {"created_at": True, "updated_at": True}
    get_grocery_catalog = Catalog.get("grocery")
    assert get_grocery_catalog.saved is True
    assert get_grocery_catalog.dict(exclude=exclude) == grocery_catalog.dict(exclude=exclude)
    assert Catalog.get("grocery").dict(exclude=exclude) == get_grocery_catalog.dict(exclude=exclude)
    assert Catalog.get("creditcard").dict(exclude=exclude) == creditcard_catalog.dict(
        exclude=exclude
    )
    assert Catalog.get_by_id(id=healthcare_catalog.id) == healthcare_catalog

    # test unexpected retrieval exception for Catalog.get
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Catalog.get("anything")
    assert "Failed to retrieve the specified object." in str(exc.value)

    # default catalog is created automatically
    default_catalog = Catalog.activate("default")
    # test list catalog names
    catalog_list = Catalog.list()
    expected_catalog_list = pd.DataFrame(
        {
            "name": [
                healthcare_catalog.name,
                creditcard_catalog.name,
                grocery_catalog.name,
                default_catalog.name,
            ],
            "created_at": [
                healthcare_catalog.created_at,
                creditcard_catalog.created_at,
                grocery_catalog.created_at,
                default_catalog.created_at,
            ],
            "active": [False, False, False, True],
        }
    )
    assert_frame_equal(catalog_list, expected_catalog_list)

    # test list with include_id=True
    catalog_list = Catalog.list(include_id=True)
    expected_catalog_list["id"] = [
        healthcare_catalog.id,
        creditcard_catalog.id,
        grocery_catalog.id,
        default_catalog.id,
    ]
    assert_frame_equal(catalog_list, expected_catalog_list[catalog_list.columns])

    # test unexpected retrieval exception for Catalog.list
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Catalog.list()
    assert "Failed to list /catalog." in str(exc.value)

    # activate a catalog
    Catalog.activate(creditcard_catalog.name)
    assert (Catalog.list()["active"] == [False, True, False, False]).all()


def test_activate():
    """
    Test Catalog.activate
    """
    # create catalogs & save to persistent
    Catalog.create(name="grocery")
    Catalog.create(name="creditcard")

    # create entity in grocery catalog
    grocery_catalog = Catalog.activate("grocery")
    assert Catalog.get_active() == grocery_catalog
    Entity(name="GroceryCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["GroceryCustomer"]

    # create entity in creditcard catalog with same serving_names
    creditcard_catalog = Catalog.activate("creditcard")
    assert Catalog.get_active() == creditcard_catalog
    Entity(name="CreditCardCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["CreditCardCustomer"]

    # switch to default catalog
    activate_catalog(DEFAULT_CATALOG_ID)
    assert Entity.list()["name"].tolist() == []


@pytest.mark.parametrize(
    "method_item",
    catalog_get_list_and_create_methods(),
)
def test_functions_are_called_from_active_catalog(method_item):
    """
    Test that catalog_obj.(list|get)_<x> functions are able to be called from the active, or inactive catalog.
    """
    method_name = method_item.class_method_delegated
    with patch.object(method_item.class_object, method_name):
        credit_card_catalog = Catalog.create("creditcard")
        grocery_catalog = Catalog.create(name="grocery")

        # Verify that there's no error even though the credit card catalog is not the current active catalog.
        # Also verify that there's no change in the global activate catalog_id.
        assert get_active_catalog_id() == grocery_catalog.id
        _invoke_method(credit_card_catalog, method_item)

        assert get_active_catalog_id() == grocery_catalog.id

        # Switch to credit card, verify no error.
        credit_card_catalog = Catalog.activate("creditcard")
        assert get_active_catalog_id() == credit_card_catalog.id
        _invoke_method(credit_card_catalog, method_item)
        assert get_active_catalog_id() == credit_card_catalog.id


def test_catalog_state_reverts_correctly_even_if_wrapped_function_errors():
    """
    Verify that the catalog state doesn't change if the wrapped function errors.
    """

    class TestCatalogError(Exception):
        """
        Internal test catalog exception class.
        """

    class TestCatalog(Catalog):
        """
        Internal test catalog class.
        """

        @update_and_reset_catalog
        def throw_error_function(self):
            """
            Internal test function that throws an error.

            Raises
            ------
            TestCatalogError
            """
            raise TestCatalogError("test")

    catalog_a = TestCatalog.create("catalog_a")
    assert get_active_catalog_id() == catalog_a.id
    catalog_b = TestCatalog.create("catalog_b")
    assert get_active_catalog_id() == catalog_b.id
    with pytest.raises(TestCatalogError):
        catalog_a.throw_error_function()
    assert get_active_catalog_id() == catalog_b.id


def test_catalog_name_synchronization_issue():
    """Test catalog name synchronization issue."""
    catalog = Catalog.create("random_catalog")
    cloned_catalog = Catalog.get("random_catalog")
    assert catalog.name == cloned_catalog.name == "random_catalog"

    catalog.update_name("updated_name")
    assert cloned_catalog.name == "updated_name"

    cloned_catalog.update_name("random_catalog")
    assert catalog.name == "random_catalog"
