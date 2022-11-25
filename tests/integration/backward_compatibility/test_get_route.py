"""
Backward compatibility tests
"""
import os
from http import HTTPStatus
from unittest import mock

import pytest
from bson.objectid import ObjectId
from fastapi.testclient import TestClient

from featurebyte.app import app
from featurebyte.persistent.mongo import MongoDB


@pytest.fixture(name="mongo_persistent", scope="module")
def mongo_persistent_fixture():
    os.system("./scripts/dump_staging_app.sh --nsInclude='app.*'")
    mongo_connection = os.getenv("MONGO_CONNECTION")
    persistent = MongoDB(uri=mongo_connection, database="app")
    yield persistent


@pytest.fixture(name="test_api_client", scope="module")
def test_api_client_fixture(mongo_persistent):
    """
    Test API client
    """
    with mock.patch("featurebyte.app.get_persistent") as mock_get_persistent:
        with mock.patch("featurebyte.app.User") as mock_user:
            mock_user.return_value.id = ObjectId()
            mock_get_persistent.return_value = mongo_persistent
            with TestClient(app) as client:
                yield client


def extract_get_routes():
    """Extract get routes to be tests"""
    routes = []
    exclude_paths = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/temp_data", "/redoc"}
    for route in app.routes:
        path = getattr(route, "path", None)
        if "{" in path or path in exclude_paths:
            continue
        if path and "GET" in getattr(route, "methods", {}):
            routes.append(path)
    return routes


def test_extract_get_routes(test_dir, update_fixtures):
    """This test is used to track the list of routes covered by backward compatibility checks"""
    fixture_path = os.path.join(test_dir, "fixtures/backward_compatibility/get_routes.txt")
    with open(fixture_path) as fhandle:
        expected_routes = sorted(line.strip() for line in fhandle.readlines())
        assert sorted(extract_get_routes()) == expected_routes

    if update_fixtures:
        routes = extract_get_routes()
        with open(fixture_path, "w") as fhandle:
            fhandle.write("\n".join(sorted(routes)))


@pytest.mark.parametrize("route", extract_get_routes())
def test_get_route(test_api_client, route):
    """Test get routes"""
    response = test_api_client.get(route)
    assert response.status_code == HTTPStatus.OK, response.text


@pytest.mark.parametrize(
    "resource_name,dependent_resources",
    [
        ("feature", ["info"]),
        ("feature_namespace", ["info"]),
        ("feature_list", ["info"]),
        ("feature_list_namespace", ["info"]),
    ],
)
def test_inner_get_routes(test_api_client, resource_name, dependent_resources):
    """Test listing routes of previously stored records: GET /<resource_name>/<resource_id>/<dependent_resource>"""
    page_size = 10
    response = test_api_client.get(f"/{resource_name}", params={"page_size": page_size})
    response_dict = response.json()
    assert response.status_code == HTTPStatus.OK

    for document in response_dict["data"]:
        for dependent_resource in dependent_resources:
            route = f"/{resource_name}/{document['_id']}/{dependent_resource}"
            response = test_api_client.get(route)
            assert response.status_code == HTTPStatus.OK, (route, response.text)
