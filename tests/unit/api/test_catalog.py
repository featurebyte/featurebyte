"""
Unit test for Catalog class
"""
from __future__ import annotations

from typing import Any, Callable, Optional

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
from featurebyte.api.catalog import Catalog
from featurebyte.api.entity import Entity
from featurebyte.api.feature import FeatureNamespace
from featurebyte.api.feature_job import FeatureJobMixin
from featurebyte.api.feature_list import FeatureListNamespace
from featurebyte.config import reset_to_default_catalog
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)


@dataclass
class MethodMetadata:
    """
    Data class to keep track of list methods to test.
    """

    # Catalog method to test
    catalog_method: Callable
    # API object which has the method (eg. list/get) we delegate to
    class_object: Any
    # Method override if we don't delegate to a standardized method (eg. list/get).
    method_override: Optional[str] = None
    # Method to patch
    method_to_patch: Optional[str] = None


def catalog_list_methods_to_test_list():
    return [
        MethodMetadata(Catalog.list_features, Feature, "_list_versions", "list_versions"),
        MethodMetadata(Catalog.list_feature_namespaces, FeatureNamespace),
        MethodMetadata(Catalog.list_feature_list_namespaces, FeatureListNamespace),
        MethodMetadata(Catalog.list_feature_lists, FeatureList, "_list_versions", "list_versions"),
        MethodMetadata(Catalog.list_tables, Table),
        MethodMetadata(Catalog.list_relationships, Relationship),
        MethodMetadata(Catalog.list_feature_job_setting_analyses, FeatureJobSettingAnalysis),
        MethodMetadata(Catalog.list_feature_stores, FeatureStore),
        MethodMetadata(Catalog.list_entities, Entity),
        MethodMetadata(Catalog.list_periodic_tasks, PeriodicTask),
    ]


def catalog_get_methods_to_test_list():
    return [
        MethodMetadata(Catalog.get_feature, Feature),
        MethodMetadata(Catalog.get_feature_namespace, FeatureNamespace),
        MethodMetadata(Catalog.get_feature_list_namespace, FeatureListNamespace),
        MethodMetadata(Catalog.get_feature_list, FeatureList),
        MethodMetadata(Catalog.get_table, Table),
        MethodMetadata(Catalog.get_relationship, Relationship),
        MethodMetadata(Catalog.get_feature_job_setting_analysis, FeatureJobSettingAnalysis),
        MethodMetadata(Catalog.get_feature_store, FeatureStore),
        MethodMetadata(Catalog.get_entity, Entity),
        MethodMetadata(Catalog.get_periodic_task, PeriodicTask),
    ]


@pytest.fixture(name="catalog_list_methods_to_test")
def catalog_list_methods_to_test_fixture():
    return catalog_list_methods_to_test_list()


@pytest.fixture(name="catalog_get_methods_to_test")
def catalog_get_methods_to_test_fixture():
    return catalog_get_methods_to_test_list()


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


def test_all_list_methods_are_exposed_in_catalog(catalog_list_methods_to_test):
    """
    Test that all inherited list methods are exposed in catalog.

    This will help to ensure that new API objects that have a list method are added to the Catalog.
    If we don't want to add them, we can add them to the excluded_children set.
    """
    api_object_children = _inheritors(ApiObject)
    excluded_children = {
        Catalog,  # accessible as part of Catalog.get
        DimensionTable,  # accessible as part of Table.get
        EventTable,  # accessible as part of Table.get
        FeatureJobMixin,
        ItemTable,  # accessible as part of Table.get
        SCDTable,  # accessible as part of Table.get
        SavableApiObject,
        TableApiObject,
        TableListMixin,
    }
    assert len(api_object_children) == len(catalog_list_methods_to_test) + len(excluded_children)

    for method_item in catalog_list_methods_to_test:
        delegated_class = method_item.class_object
        assert delegated_class in api_object_children


@pytest.mark.parametrize(
    "method_item",
    catalog_list_methods_to_test_list(),
)
def test_list_methods_have_same_parameters_as_delegated_list_method_call(method_item):
    """
    Test catalog list methods have same parameters as underlying methods.
    This will help to ensure that the Catalog list APIs are consistent with their API object List methods.
    """
    catalog_list_method, underlying_class = method_item.catalog_method, method_item.class_object
    # Check that the signatures match
    catalog_list_method_signature = signature(catalog_list_method)
    underlying_class_method = underlying_class.list
    if method_item.method_override:
        underlying_class_method = getattr(underlying_class, method_item.method_override)
    underlying_class_list_method_signature = signature(underlying_class_method)
    assert [*catalog_list_method_signature.parameters.keys()] == [
        "self",
        *underlying_class_list_method_signature.parameters.keys(),
    ], f"catalog method: {catalog_list_method}, underlying_class {underlying_class}"


@pytest.mark.parametrize(
    "method_item",
    catalog_list_methods_to_test_list(),
)
def test_list_methods_call_the_correct_delegated_method(method_item):
    """
    Test catalog list methods call the correct delegated method.
    """
    # Assert that the delegated list method is called
    method_name = method_item.method_to_patch or "list"
    catalog = Catalog.create("random")
    with patch.object(method_item.class_object, method_name) as mocked_list:
        catalog_method_to_call = getattr(catalog, method_item.catalog_method.__name__)
        catalog_method_to_call()
        mocked_list.assert_called()


def test_all_get_methods_are_exposed_in_catalog(catalog_get_methods_to_test):
    """
    Test that all inherited get methods are exposed in catalog.

    This will help to ensure that new API objects that have a get method are added to the Catalog.
    If we don't want to add them, we can add them to the excluded_children set.
    """
    api_object_children = _inheritors(ApiObject)
    excluded_children = {
        Catalog,  # accessible as part of Catalog.get
        DimensionTable,  # accessible as part of Table.get
        EventTable,  # accessible as part of Table.get
        FeatureJobMixin,
        ItemTable,  # accessible as part of Table.get
        SCDTable,  # accessible as part of Table.get
        SavableApiObject,
        TableApiObject,
        TableListMixin,
    }
    assert len(api_object_children) == len(catalog_get_methods_to_test) + len(excluded_children)

    for method_item in catalog_get_methods_to_test:
        delegated_class = method_item.class_object
        assert delegated_class in api_object_children


@pytest.mark.parametrize(
    "method_item",
    catalog_get_methods_to_test_list(),
)
def test_get_methods_have_same_parameters_as_delegated_get_method_call(method_item):
    """
    Test catalog get methods have same parameters as underlying methods.
    This will help to ensure that the Catalog `get_*` APIs are consistent with their API object `get` methods.
    """
    catalog_get_method, underlying_class = method_item.catalog_method, method_item.class_object
    # Check that the signatures match
    catalog_get_method_signature = signature(catalog_get_method)
    underlying_class_method = underlying_class.get
    if method_item.method_override:
        underlying_class_method = getattr(underlying_class, method_item.method_override)
    underlying_class_get_method_signature = signature(underlying_class_method)
    assert [*catalog_get_method_signature.parameters.keys()] == [
        "self",
        *underlying_class_get_method_signature.parameters.keys(),
    ], f"catalog method: {catalog_get_method}, underlying_class {underlying_class}"


@pytest.mark.parametrize(
    "method_item",
    catalog_get_methods_to_test_list(),
)
def test_get_methods_call_the_correct_delegated_method(method_item):
    """
    Test catalog get methods call the correct delegated method.
    """
    # Assert that the delegated get method is called
    method_name = method_item.method_to_patch or "get"
    catalog = Catalog.create("random")
    with patch.object(method_item.class_object, method_name) as mocked_get:
        catalog_method_to_call = getattr(catalog, method_item.catalog_method.__name__)
        catalog_method_to_call("random_name")
        mocked_get.assert_called()


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
    reset_to_default_catalog()


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
    reset_to_default_catalog()
    assert Entity.list()["name"].tolist() == []


def _assert_function_is_not_called_from_active_catalog(function):
    """
    Assert that the function is not called from an active catalog.

    We verify this by calling the function and making sure that it raises an error. This makes an assumption that the
    currently active catalog is not the catalog that this function is being invoked from.
    """
    with pytest.raises(ValueError) as exc:
        function()
    assert "Catalog is not active" in str(exc.value)


def _get_list_functions():
    all_functions = dir(Catalog)
    return [f for f in all_functions if f.startswith("list_")]


@pytest.mark.parametrize("list_function", _get_list_functions())
def test_list_functions_are_called_from_active_catalog(list_function):
    """
    Test that list_<x> functions are called from the active catalog.
    """
    credit_card_catalog = Catalog.create("creditcard")
    grocery_catalog = Catalog.create(name="grocery")

    _assert_function_is_not_called_from_active_catalog(getattr(credit_card_catalog, list_function))

    # Verify no error
    grocery_catalog.list_features()

    # Switch to credit card, verify no error.
    credit_card_catalog = Catalog.activate("creditcard")
    credit_card_catalog.list_features()

    reset_to_default_catalog()
