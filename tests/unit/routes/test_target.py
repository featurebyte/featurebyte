"""
Test for target routes
"""
from http import HTTPStatus
from unittest import mock

import pandas as pd
from bson import ObjectId

from featurebyte.models import EntityModel
from featurebyte.models.entity import ParentEntity
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestTargetApi(BaseCatalogApiTestSuite):
    """
    TestTargetApi class
    """

    class_name = "Target"
    base_route = "/target"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/target.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Target (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Target.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'TargetNamespace (name: "float_target") already exists. '
            'Please rename object (name: "float_target") to something else.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "node_name": ["cust_id"]},
            [
                {
                    "loc": ["body", "node_name"],
                    "msg": "str type expected",
                    "type": "type_error.str",
                }
            ],
        )
    ]
    create_parent_unprocessable_payload_expected_detail_pairs = [
        (
            {
                "id": str(unknown_id),
                "table_type": "event_table",
                "table_id": str(ObjectId()),
            },
            f'Target (id: "{unknown_id}") not found. Please save the Target object first.',
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)

        # check target namespace
        test_api_client, _ = test_api_client_persistent
        default_catalog_id = test_api_client.headers["active-catalog-id"]
        create_response_dict = create_success_response.json()
        namespace_id = create_response_dict["target_namespace_id"]
        response = test_api_client.get(f"/target_namespace/{namespace_id}")
        response_dict = response.json()
        assert response_dict == {
            "_id": namespace_id,
            "name": "float_target",
            "dtype": "FLOAT",
            "target_ids": [create_response_dict["_id"]],
            "window": "1d",
            "default_target_id": create_response_dict["_id"],
            "default_version_mode": "AUTO",
            "entity_ids": response_dict["entity_ids"],
            "catalog_id": str(default_catalog_id),
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "user_id": str(user_id),
            "block_modification_by": [],
            "description": None,
        }

    def test_create_target__entity_parent_id_in_the_list(
        self,
        create_success_response,
        test_api_client_persistent,
    ):
        """
        Test context update (unprocessable)
        """
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response.json()
        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        entity_payload["serving_names"] = [entity_payload["serving_name"]]

        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["name"] = f"{payload['name']}_1"

        with mock.patch("featurebyte.service.entity.EntityService.get_document") as mock_get_doc:
            mock_get_doc.return_value = EntityModel(
                **entity_payload,
                parents=[
                    ParentEntity(
                        id=ObjectId(entity_payload["_id"]),
                        table_type="event_table",
                        table_id=ObjectId(),
                    )
                ],
            )
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
            assert (
                "Target entity ids must not include any parent entity ids"
                in response.json()["detail"]
            )

    def test_request_sample_entity_serving_names(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test getting sample entity serving names for a feature"""
        test_api_client, _ = test_api_client_persistent
        result = create_success_response.json()

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame(
                [
                    {
                        "cust_id": 1,
                    },
                    {
                        "cust_id": 2,
                    },
                    {
                        "cust_id": 3,
                    },
                ]
            )

        mock_session = mock_get_session.return_value
        mock_session.execute_query = mock_execute_query

        # Request sample entity serving names
        target_id = result["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{target_id}/sample_entity_serving_names?count=10",
        )

        # Check result
        assert response.status_code == HTTPStatus.OK, response.content
        assert response.json() == {
            "entity_serving_names": [
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
            ],
        }
