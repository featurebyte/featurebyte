"""
Test for exposure routes
"""

from http import HTTPStatus

import pandas as pd
from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestExposureApi(BaseCatalogApiTestSuite):
    """
    TestExposureApi class
    """

    class_name = "Exposure"
    base_route = "/exposure"
    unknown_id = ObjectId()
    # Reuse the target.json payload structure for exposure (same graph structure)
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/target.json")
    # Override name to avoid conflicts with target
    payload = {**payload, "_id": str(ObjectId()), "name": "float_exposure"}

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Exposure (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Exposure.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "node_name": ["cust_id"]},
            [
                {
                    "input": ["cust_id"],
                    "loc": ["body", "node_name"],
                    "msg": "Input should be a valid string",
                    "type": "string_type",
                }
            ],
        ),
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

            if api_object.endswith("_table"):
                # tag table entity for table objects
                self.tag_table_entity(api_client, api_object, payload)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{self.payload['name']}_{i}"
            yield payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)

        # check exposure namespace was created
        test_api_client, _ = test_api_client_persistent
        default_catalog_id = test_api_client.headers["active-catalog-id"]
        create_response_dict = create_success_response.json()
        namespace_id = create_response_dict["exposure_namespace_id"]
        response = test_api_client.get(f"/exposure_namespace/{namespace_id}")
        response_dict = response.json()
        assert response_dict["name"] == "float_exposure"
        assert response_dict["dtype"] == "FLOAT"
        assert response_dict["exposure_ids"] == [create_response_dict["_id"]]
        assert response_dict["default_exposure_id"] == create_response_dict["_id"]
        assert response_dict["default_version_mode"] == "AUTO"
        assert response_dict["catalog_id"] == str(default_catalog_id)

    def test_request_sample_entity_serving_names(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test getting sample entity serving names for an exposure"""
        test_api_client, _ = test_api_client_persistent
        result = create_success_response.json()

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([
                {"cust_id": 1},
                {"cust_id": 2},
                {"cust_id": 3},
            ])

        mock_session = mock_get_session.return_value
        mock_session.execute_query = mock_execute_query

        exposure_id = result["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{exposure_id}/sample_entity_serving_names?count=10",
        )

        assert response.status_code == HTTPStatus.OK, response.content
        assert "entity_serving_names" in response.json()

    def test_delete_exposure_namespace(self, test_api_client_persistent, create_success_response):
        """Test delete exposure namespace when exposure exists"""
        test_api_client, _ = test_api_client_persistent
        namespace_id = create_success_response.json()["exposure_namespace_id"]
        response = test_api_client.delete(f"/exposure_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "ExposureNamespace is referenced by Exposure: float_exposure"
        )

    def test_delete_exposure(self, test_api_client_persistent, create_success_response):
        """Test delete exposure"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        exposure_id, namespace_id = (
            response_dict["_id"],
            response_dict["exposure_namespace_id"],
        )
        response = test_api_client.delete(f"/exposure/{exposure_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check that exposure is deleted but namespace is not
        response = test_api_client.get(f"/exposure/{exposure_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
        response = test_api_client.get(f"/exposure_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["exposure_ids"] == [], response.json()

    def test_get_info(self, test_api_client_persistent, create_success_response):
        """Test getting exposure info"""
        test_api_client, _ = test_api_client_persistent
        exposure_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{exposure_id}/info")
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["exposure_name"] == "float_exposure"
