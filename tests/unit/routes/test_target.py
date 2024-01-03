"""
Test for target routes
"""
from http import HTTPStatus
from unittest import mock
from unittest.mock import Mock

import pandas as pd
import pytest
from bson import ObjectId
from pandas._testing import assert_frame_equal

from featurebyte.common.utils import dataframe_from_json
from featurebyte.models import EntityModel
from featurebyte.models.entity import ParentEntity
from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.target_table import TargetTableCreate
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

    @pytest.fixture(name="target_preview_payload")
    def target_preview_payload_fixture(self, create_success_response):
        """
        target_preview_payload fixture
        """
        target = create_success_response.json()
        return {
            "graph": target["graph"],
            "node_name": target["node_name"],
            "point_in_time_and_serving_name_list": [
                {
                    "cust_id": "C1",
                    "POINT_IN_TIME": "2022-04-01",
                },
                {
                    "cust_id": "C3",
                    "POINT_IN_TIME": "2022-04-03",
                },
            ],
            "feature_store_id": self.payload["tabular_source"]["feature_store_id"],
        }

    def test_preview_200(
        self,
        test_api_client_persistent,
        target_preview_payload,
        mock_get_session,
    ):
        """Test target preview (success)"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using graph and node name
        response = test_api_client.post(f"{self.base_route}/preview", json=target_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_using_target_id_200(
        self,
        test_api_client_persistent,
        create_success_response,
        target_preview_payload,
        mock_get_session,
    ):
        """Test target preview (success)"""
        test_api_client, _ = test_api_client_persistent
        target = create_success_response.json()
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using target id
        target_preview_payload.pop("graph")
        target_preview_payload.pop("node_name")
        target_preview_payload["target_id"] = target["_id"]
        response = test_api_client.post(f"{self.base_route}/preview", json=target_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_delete_entity(self, test_api_client_persistent, create_success_response):
        """Test delete entity"""
        test_api_client, _ = test_api_client_persistent
        entity_id = create_success_response.json()["entity_ids"][0]
        response = test_api_client.delete(f"/entity/{entity_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Entity is referenced by Target: float_target"

    def test_delete_target_namespace(self, test_api_client_persistent, create_success_response):
        """Test delete target namespace"""
        test_api_client, _ = test_api_client_persistent
        namespace_id = create_success_response.json()["target_namespace_id"]
        response = test_api_client.delete(f"/target_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "TargetNamespace is referenced by Target: float_target"

    def test_delete_target(self, test_api_client_persistent, create_success_response):
        """Test delete target"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        target_id, namespace_id = response_dict["_id"], response_dict["target_namespace_id"]
        response = test_api_client.delete(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check that target is deleted but namespace is not
        response = test_api_client.get(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
        response = test_api_client.get(f"/target_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["target_ids"] == [], response.json()

    @pytest.mark.asyncio
    async def test_creating_target_table_with_just_target_id(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """
        Test that we can create a target table without a graph and node_names, but with just the target id.
        """
        test_api_client, _ = test_api_client_persistent
        target = create_success_response.json()
        # Create an observation table
        obs_table_id = ObjectId()
        await create_observation_table(obs_table_id)

        # Create payload with no graph and no node names
        create = TargetTableCreate(
            name="target_name",
            feature_store_id=ObjectId(),
            serving_names_mapping={},
            target_id=target["_id"],
            context_id=None,
            request_input=UploadedFileInput(
                type=RequestInputType.UPLOADED_FILE,
                file_name="random_file_name",
            ),
            observation_table_id=obs_table_id,
        )
        data = {"payload": create.json()}

        with mock.patch(
            "featurebyte.routes.common.feature_or_target_table.FeatureOrTargetTableController.create_table"
        ) as mock_create_table:
            test_api_client.post("/target_table", data=data)
            assert mock_create_table.call_count == 1
            call_args = mock_create_table.call_args_list[0][1]
            # Check that node names is in the call args of mock_create_table
            assert call_args["data"].node_names == ["project_1"]
            assert call_args["data"].graph is not None
