"""
Tests for Entity route
"""
from datetime import datetime
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId
from freezegun import freeze_time


@pytest.fixture(name="entity_dict")
def entity_dict_fixture():
    """
    Entity dictionary fixture
    """
    return {"_id": str(ObjectId()), "name": "customer", "serving_name": "cust_id"}


@pytest.fixture(name="create_success_response")
def create_success_response_fixture(test_api_client_persistent, entity_dict):
    """
    Post create success response fixture
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.post("/entity", json=entity_dict)
    return response


@pytest.fixture(name="create_multiple_entries")
def create_multiple_entries_fixture(test_api_client_persistent):
    """
    Create multiple entries to the persistent
    """
    test_api_client, _ = test_api_client_persistent
    entity_id1, entity_id2, entity_id3 = str(ObjectId()), str(ObjectId()), str(ObjectId())
    res_region = test_api_client.post(
        "/entity", json={"_id": entity_id1, "name": "region", "serving_name": "region"}
    )
    res_cust = test_api_client.post(
        "/entity", json={"_id": entity_id2, "name": "customer", "serving_name": "cust_id"}
    )
    res_prod = test_api_client.post(
        "/entity", json={"_id": entity_id3, "name": "product", "serving_name": "prod_id"}
    )
    assert res_region.status_code == HTTPStatus.CREATED
    assert res_cust.status_code == HTTPStatus.CREATED
    assert res_prod.status_code == HTTPStatus.CREATED
    assert res_region.json()["_id"] == entity_id1
    assert res_cust.json()["_id"] == entity_id2
    assert res_prod.json()["_id"] == entity_id3
    return [entity_id1, entity_id2, entity_id3]


def test_create_201(create_success_response):
    """
    Test entity creation (success)
    """
    utcnow = datetime.utcnow()
    assert create_success_response.status_code == HTTPStatus.CREATED
    result = create_success_response.json()

    # check response
    _ = ObjectId(result.pop("_id"))  # valid ObjectId
    assert result.pop("user_id") is None
    assert datetime.fromisoformat(result.pop("created_at")) < utcnow


def test_create_409(create_success_response, test_api_client_persistent, entity_dict):
    """
    Test entity creation (conflict)
    """
    test_api_client, _ = test_api_client_persistent
    _ = create_success_response
    response = test_api_client.post("/entity", json=entity_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'Entity name (entity.name: "customer") already exists.'}

    entity_dict["name"] = "Customer"
    response = test_api_client.post("/entity", json=entity_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": 'Entity serving name (entity.serving_names: "cust_id") already exists.'
    }


def test_create_422(test_api_client_persistent, entity_dict):
    """
    Test entity creation (unprocessable entity)
    """
    test_api_client, _ = test_api_client_persistent
    entity_dict["serving_name"] = ["cust_id"]
    response = test_api_client.post("/entity", json=entity_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "serving_name"],
                "msg": "str type expected",
                "type": "type_error.str",
            }
        ]
    }


def test_list_200(create_multiple_entries, test_api_client_persistent):
    """
    Test list entities (success)
    """
    test_api_client, _ = test_api_client_persistent
    _ = create_multiple_entries
    response = test_api_client.get("/entity")
    assert response.status_code == HTTPStatus.OK
    result = response.json()
    expected_paginated_info = {"page": 1, "page_size": 10, "total": 3}

    result_data = result.pop("data")
    expected_sorted_name_desc = ["product", "customer", "region"]
    expected_sorted_serv_name_desc = [["prod_id"], ["cust_id"], ["region"]]
    assert all(elem.get("_id") is not None for elem in result_data)
    assert [elem["name"] for elem in result_data] == expected_sorted_name_desc
    assert [elem["serving_names"] for elem in result_data] == expected_sorted_serv_name_desc
    assert result == expected_paginated_info

    # test with route params
    response_with_params = test_api_client.get(
        "/entity", params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 1}
    )
    assert response_with_params.status_code == HTTPStatus.OK
    result = response_with_params.json()
    expected_paginated_info = {"page": 1, "page_size": 2, "total": 3}

    result_data = result.pop("data")
    expected_sorted_name_asc = ["customer", "product"]
    expected_sorted_serv_name_asc = [["cust_id"], ["prod_id"]]
    assert [elem["name"] for elem in result_data] == expected_sorted_name_asc
    assert [elem["serving_names"] for elem in result_data] == expected_sorted_serv_name_asc
    assert result == expected_paginated_info

    # test search with entity name
    response_with_entity_name = test_api_client.get("/entity", params={"name": "customer"})
    assert response_with_entity_name.status_code == HTTPStatus.OK
    result = response_with_entity_name.json()
    result_data = result["data"]
    assert len(result_data) == 1
    assert result_data[0]["name"] == "customer"


def test_get_200(create_success_response, test_api_client_persistent):
    """
    Test get entities (success)
    """
    test_api_client, _ = test_api_client_persistent
    created_entity = create_success_response.json()
    entity_id = created_entity["_id"]
    response = test_api_client.get(f"/entity/{entity_id}")
    response_data = response.json()
    response_data.pop("created_at")
    response_data.pop("updated_at")
    assert response_data == {
        "_id": entity_id,
        "name": "customer",
        "serving_names": ["cust_id"],
        "name_history": [],
        "user_id": None,
    }


def test_get_404(test_api_client_persistent):
    """
    Test get entities (not found)
    """
    test_api_client, _ = test_api_client_persistent
    unknown_entity_id = ObjectId()
    response = test_api_client.get(f"/entity/{unknown_entity_id}")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": (
            f'Entity (entity.id: "{unknown_entity_id}") not found! Please save the Entity object first.'
        )
    }


@freeze_time("2022-07-01")
def test_update_200(create_success_response, test_api_client_persistent):
    """
    Test entity update (success)
    """
    test_api_client, _ = test_api_client_persistent
    response_dict = create_success_response.json()
    entity_id = response_dict["_id"]
    response = test_api_client.patch(f"/entity/{entity_id}", json={"name": "Customer"})
    assert response.status_code == HTTPStatus.OK
    result = response.json()

    assert result["name"] == "Customer"
    assert result["name_history"] == [{"created_at": "2022-07-01T00:00:00", "name": "customer"}]
    for key in result.keys():
        if key not in {"name", "name_history", "updated_at"}:
            assert result[key] == response_dict[key]

    # test special case when the name is the same, should not update name history
    response = test_api_client.patch(f"/entity/{entity_id}", json={"name": "Customer"})
    assert response.status_code == HTTPStatus.OK
    assert response.json() == result


def test_update_404(test_api_client_persistent):
    """
    Test entity update (not found)
    """
    test_api_client, _ = test_api_client_persistent
    unknown_entity_id = ObjectId()
    response = test_api_client.patch(f"/entity/{unknown_entity_id}", json={"name": "random_name"})
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": (
            f'Entity (entity.id: "{unknown_entity_id}") not found! Please save the Entity object first.'
        )
    }


def test_update_409(create_multiple_entries, test_api_client_persistent):
    """ "
    Test entity update (conflict)
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.patch(
        f"/entity/{create_multiple_entries[0]}", json={"name": "customer"}
    )
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'Entity name (entity.name: "customer") already exists.'}


def test_update_422(test_api_client_persistent):
    """
    Test entity update (unprocessable entity)
    """
    test_api_client, _ = test_api_client_persistent
    unknown_entity_id = ObjectId()
    response = test_api_client.patch(f"/entity/{unknown_entity_id}")
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [
            {
                "loc": ["body"],
                "msg": "field required",
                "type": "value_error.missing",
            }
        ]
    }
