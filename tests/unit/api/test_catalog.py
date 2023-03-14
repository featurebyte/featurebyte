"""
Unit test for Catalog class
"""
from __future__ import annotations

from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from featurebyte.api.catalog import Catalog
from featurebyte.api.entity import Entity
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)


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

    # test list catalog names
    catalog_list = Catalog.list()
    # default catalog is created automatically
    default_catalog = Catalog.get("default")
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
    Catalog.activate_catalog(creditcard_catalog.name)
    assert (Catalog.list()["active"] == [False, True, False, False]).all()


def test_activate_catalog():
    """
    Test Catalog.activate
    """
    # create catalogs & save to persistent
    grocery_catalog = Catalog.create(name="grocery")
    creditcard_catalog = Catalog.create(name="creditcard")

    # create entity in grocery catalog
    grocery_catalog.activate()
    assert Catalog.get_active() == grocery_catalog
    Entity(name="GroceryCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["GroceryCustomer"]

    # create entity in creditcard catalog with same serving_names
    Catalog.activate_catalog("creditcard")
    assert Catalog.get_active() == creditcard_catalog
    Entity(name="CreditCardCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["CreditCardCustomer"]

    # switch to default catalog
    Catalog.activate_catalog("default")
    assert Entity.list()["name"].tolist() == []
