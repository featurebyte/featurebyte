"""
Test relationship info routes
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import User
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.relationship_info import RelationshipInfoCreate, RelationshipInfoUpdate
from featurebyte.storage import LocalTempStorage
from featurebyte.worker import get_celery
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestRelationshipInfoApi(BaseCatalogApiTestSuite):
    """
    Test relationship info routes
    """

    class_name = "RelationshipInfo"
    base_route = "/relationship_info"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/relationship_info.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        # Create 3 distinct pairs of entity IDs so that the payload generated here is unique.
        entity_ids = [
            "63f94ed6ea1f050131379214",
            "63f94ed6ea1f050131379204",
            "63f94ed6ea1f050131379204",
            "63f6a145e549df8ccf2bf3f2",
            "63f6a145e549df8ccf2bf3f2",
            "63f6a145e549df8ccf2bf3f3",
        ]
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["entity_id"] = entity_ids[i * 2]
            payload["related_entity_id"] = entity_ids[i * 2 + 1]
            payload["relation_table_id"] = "6337f9651050ee7d5980660d"
            payload["updated_by"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("event_table", "event_table"),
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("entity", "entity_user"),
            ("entity", "entity_country"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    @staticmethod
    def create_app_container(persistent, user_id, catalog_id):
        """App container fixture"""
        user = User(id=user_id)
        return LazyAppContainer(
            user=user,
            persistent=persistent,
            temp_storage=LocalTempStorage(),
            celery=get_celery(),
            storage=LocalTempStorage(),
            catalog_id=catalog_id,
            app_container_config=app_container_config,
        )

    @pytest_asyncio.fixture
    async def create_success_response(
        self, test_api_client_persistent, user_id, default_catalog_id
    ):  # pylint: disable=arguments-differ
        """Post a relationship info"""
        test_api_client, persistent = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        app_container = self.create_app_container(
            persistent=persistent, user_id=user_id, catalog_id=ObjectId(default_catalog_id)
        )

        # create relationship info
        relationship_info = (
            await app_container.relationship_info_controller.create_relationship_info(
                data=RelationshipInfoCreate(**self.payload)
            )
        )
        response = test_api_client.get(f"{self.base_route}/{relationship_info.id}")
        assert response.status_code == HTTPStatus.OK, response.text
        return response

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(
        self, test_api_client_persistent, user_id, default_catalog_id
    ):  # pylint: disable=arguments-differ
        """Post multiple relationship info"""
        test_api_client, persistent = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        app_container = self.create_app_container(
            persistent=persistent, user_id=user_id, catalog_id=ObjectId(default_catalog_id)
        )

        # create multiple relationship info
        output = []
        for payload in self.multiple_success_payload_generator(test_api_client):
            relationship_info = (
                await app_container.relationship_info_controller.create_relationship_info(
                    data=RelationshipInfoCreate(**payload)
                )
            )
            response = test_api_client.get(f"{self.base_route}/{relationship_info.id}")
            assert response.status_code == HTTPStatus.OK, response.text
            output.append(response)
        return output

    @pytest.mark.skip("POST method not exposed")
    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        """Test creation (success) without specifying id field"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__id_is_none(self, test_api_client_persistent):
        """Test creation (success) ID is None"""

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent

        # Create relationship info
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # Get info
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict["relationship_type"] == "child_parent"
        assert response_dict["entity_name"] == "customer"
        assert response_dict["related_entity_name"] == "transaction"
        assert response_dict["table_name"] == "sf_event_table"

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """
        Test patch to update enabled status
        """
        # Create RelationshipInfo and verify enabled is True
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        assert response_dict["enabled"]

        # Update enabled to False
        data_update = RelationshipInfoUpdate(enabled=False)
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}", json=data_update.dict()
        )
        assert response.status_code == HTTPStatus.OK

        # Verify enabled is False
        response = test_api_client.get(f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert not response_dict["enabled"]
