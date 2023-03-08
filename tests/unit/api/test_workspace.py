"""
Unit test for Workspace class
"""
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pydantic import ValidationError

from featurebyte.api.entity import Entity
from featurebyte.api.workspace import Workspace
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)


@pytest.fixture(name="workspace")
def workspace_fixture():
    """
    Workspace fixture
    """
    workspace = Workspace(name="grocery")
    previous_id = workspace.id
    assert workspace.saved is False
    workspace.save()
    assert workspace.saved is True
    assert workspace.id == previous_id
    yield workspace


def test_workspace_creation__input_validation():
    """
    Test workspace creation input validation
    """
    with pytest.raises(ValidationError) as exc:
        Workspace(name=123)
    assert "str type expected (type=type_error.str)" in str(exc.value)


def test_workspace__update_name(workspace):
    """
    Test update_name in Workspace class
    """
    # test update name (saved object)
    assert workspace.name == "grocery"
    workspace.update_name("Grocery")
    assert workspace.name == "Grocery"
    assert workspace.saved is True

    # test update name (non-saved object)
    another_workspace = Workspace(name="CreditCard")
    with pytest.raises(RecordRetrievalException) as exc:
        Workspace.get("CreditCard")
    expected_msg = (
        'Workspace (name: "CreditCard") not found. ' "Please save the Workspace object first."
    )
    assert expected_msg in str(exc.value)
    assert another_workspace.name == "CreditCard"
    another_workspace.update_name("creditcard")
    assert another_workspace.name == "creditcard"
    assert another_workspace.saved is False


def test_info(workspace):
    """
    Test info
    """
    info_dict = workspace.info(verbose=True)
    expected_info = {
        "name": "grocery",
        "updated_at": None,
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


def test_workspace_creation(workspace):
    """
    Test workspace creation
    """
    assert workspace.name == "grocery"
    name_history = workspace.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "grocery"}.items()

    with pytest.raises(DuplicatedRecordException) as exc:
        Workspace(name="grocery").save()
    expected_msg = (
        'Workspace (name: "grocery") already exists. '
        'Get the existing object by `Workspace.get(name="grocery")`.'
    )
    assert expected_msg in str(exc.value)


def test_workspace_update_name(workspace):
    """
    Test update workspace name
    """
    name_history = workspace.name_history
    assert len(name_history) == 1
    assert name_history[0].items() > {"name": "grocery"}.items()

    workspace_id = workspace.id
    tic = datetime.utcnow()
    workspace.update_name("Grocery")
    toc = datetime.utcnow()
    assert workspace.id == workspace_id
    name_history = workspace.name_history
    assert len(name_history) == 2
    assert name_history[0].items() >= {"name": "Grocery"}.items()
    assert toc > datetime.fromisoformat(name_history[0]["created_at"]) > tic
    assert name_history[1].items() >= {"name": "grocery"}.items()
    assert tic > datetime.fromisoformat(name_history[1]["created_at"])

    # check audit history
    audit_history = workspace.audit()
    expected_audit_history = pd.DataFrame(
        [
            ("UPDATE", 'update: "grocery"', "name", "grocery", "Grocery"),
            ("UPDATE", 'update: "grocery"', "updated_at", None, workspace.updated_at.isoformat()),
            ("INSERT", 'insert: "grocery"', "created_at", np.nan, workspace.created_at.isoformat()),
            ("INSERT", 'insert: "grocery"', "name", np.nan, "grocery"),
            ("INSERT", 'insert: "grocery"', "updated_at", np.nan, None),
            ("INSERT", 'insert: "grocery"', "user_id", np.nan, None),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )

    # create another workspace
    Workspace(name="creditcard").save()

    with pytest.raises(TypeError) as exc:
        workspace.update_name(type)
    assert 'type of argument "name" must be str; got type instead' in str(exc.value)

    with pytest.raises(DuplicatedRecordException) as exc:
        workspace.update_name("creditcard")
    assert exc.value.response.json() == {
        "detail": (
            'Workspace (name: "creditcard") already exists. '
            'Get the existing object by `Workspace.get(name="creditcard")`.'
        )
    }

    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordUpdateException):
            workspace.update_name("hello")


def test_get_workspace():
    """
    Test Workspace.get function
    """
    # create workspaces & save to persistent
    grocery_workspace = Workspace(name="grocery")
    creditcard_workspace = Workspace(name="creditcard")
    healthcare_workspace = Workspace(name="healthcare")
    grocery_workspace.save()
    creditcard_workspace.save()
    healthcare_workspace.save()

    # load the workspaces from the persistent
    exclude = {"created_at": True, "updated_at": True}
    get_grocery_workspace = Workspace.get("grocery")
    assert get_grocery_workspace.saved is True
    assert get_grocery_workspace.dict(exclude=exclude) == grocery_workspace.dict(exclude=exclude)
    assert Workspace.get("grocery").dict(exclude=exclude) == get_grocery_workspace.dict(
        exclude=exclude
    )
    assert Workspace.get("creditcard").dict(exclude=exclude) == creditcard_workspace.dict(
        exclude=exclude
    )
    assert Workspace.get_by_id(id=healthcare_workspace.id) == healthcare_workspace

    # test unexpected retrieval exception for Workspace.get
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Workspace.get("anything")
    assert "Failed to retrieve the specified object." in str(exc.value)

    # test list workspace names
    workspace_list = Workspace.list()
    # default workspace is created automatically
    default_workspace = Workspace.get("default")
    expected_workspace_list = pd.DataFrame(
        {
            "name": [
                healthcare_workspace.name,
                creditcard_workspace.name,
                grocery_workspace.name,
                default_workspace.name,
            ],
            "created_at": [
                healthcare_workspace.created_at,
                creditcard_workspace.created_at,
                grocery_workspace.created_at,
                default_workspace.created_at,
            ],
            "active": [False, False, False, True],
        }
    )
    assert_frame_equal(workspace_list, expected_workspace_list)

    # test list with include_id=True
    workspace_list = Workspace.list(include_id=True)
    expected_workspace_list["id"] = [
        healthcare_workspace.id,
        creditcard_workspace.id,
        grocery_workspace.id,
        default_workspace.id,
    ]
    assert_frame_equal(workspace_list, expected_workspace_list[workspace_list.columns])

    # test unexpected retrieval exception for Workspace.list
    with mock.patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            Workspace.list()
    assert "Failed to list /workspace." in str(exc.value)

    # activate a workspace
    Workspace.activate_workspace(creditcard_workspace.name)
    assert (Workspace.list()["active"] == [False, True, False, False]).all()


def test_activate_workspace():
    """
    Test Workspace.activate
    """
    # create workspaces & save to persistent
    grocery_workspace = Workspace.create(name="grocery")
    creditcard_workspace = Workspace.create(name="creditcard")

    # create entity in grocery workspace
    grocery_workspace.activate()
    assert Workspace.get_active() == grocery_workspace
    Entity(name="GroceryCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["GroceryCustomer"]

    # create entity in creditcard workspace with same serving_names
    Workspace.activate_workspace("creditcard")
    assert Workspace.get_active() == creditcard_workspace
    Entity(name="CreditCardCustomer", serving_names=["cust_id"]).save()
    assert Entity.list()["name"].tolist() == ["CreditCardCustomer"]

    # switch to default workspace
    Workspace.activate_workspace("default")
    assert Entity.list()["name"].tolist() == []
