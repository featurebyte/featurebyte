"""
Test for FeatureNamespace route
"""
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.schema.feature import FeatureCreate
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureNamespaceApi(BaseApiTestSuite):
    """
    TestFeatureNamespaceApi
    """

    class_name = "FeatureNamespace"
    base_route = "/feature_namespace"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'FeatureNamespace (id: "{payload["_id"]}") already exists.'),
        (
            {**payload, "_id": str(ObjectId())},
            (
                'FeatureNamespace (name: "sum_30m") already exists. '
                'Please rename object (name: "sum_30m") to something else.'
            ),
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {key: value for key, value in payload.items() if key != "name"},
            [{"loc": ["body", "name"], "msg": "field required", "type": "value_error.missing"}],
        )
    ]
    not_found_save_suggestion = False

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload

    async def setup_get_info(self, api_client, persistent, user_id):
        """Setup for get_info route testing"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        payload = self.load_payload(f"tests/fixtures/request_payloads/feature_sum_30m.json")
        await persistent.insert_one(
            collection_name="feature",
            document=FeatureCreate(**payload).dict(by_alias=True),
            user_id=user_id,
        )

    @pytest.mark.asyncio
    async def test_update_200(self, test_api_client_persistent, create_success_response):
        """
        Test update (success)
        """
        test_api_client, persistent = test_api_client_persistent
        feature_namespace_data_before_update, _ = await persistent.find("feature_namespace", {})
        create_success_response_dict = create_success_response.json()
        feature_ids_before = feature_namespace_data_before_update[0]["feature_ids"]
        assert len(feature_ids_before) == 1

        # insert a feature_id to feature collection
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document={
                "_id": ObjectId(),
                "user_id": ObjectId(create_success_response_dict["user_id"]),
                "name": create_success_response_dict["name"],
                "entity_ids": [ObjectId(eid) for eid in create_success_response_dict["entity_ids"]],
                "event_data_ids": [
                    ObjectId(eid) for eid in create_success_response_dict["event_data_ids"]
                ],
                "readiness": FeatureReadiness.DRAFT.value,
            },
        )
        feature_data, _ = await persistent.find(collection_name="feature", query_filter={})
        assert len(feature_data) == 1

        response = test_api_client.patch(
            f'{self.base_route}/{create_success_response_dict["_id"]}',
            json={"feature_id": str(feature_id)},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["user_id"] == create_success_response_dict["user_id"]
        assert response_dict["feature_ids"] == [str(feature_ids_before[0]), str(feature_id)]
        assert response_dict["readiness"] == FeatureReadiness.DRAFT
        assert response_dict["default_feature_id"] == str(feature_id)
        assert response_dict["default_version_mode"] == DefaultVersionMode.AUTO

        # update another feature_id with lower readiness level
        worse_readiness_feature_id = await persistent.insert_one(
            collection_name="feature",
            document={
                "_id": ObjectId(),
                "user_id": ObjectId(create_success_response_dict["user_id"]),
                "name": create_success_response_dict["name"],
                "entity_ids": [ObjectId(eid) for eid in create_success_response_dict["entity_ids"]],
                "event_data_ids": [
                    ObjectId(eid) for eid in create_success_response_dict["event_data_ids"]
                ],
                "readiness": FeatureReadiness.DEPRECATED.value,
            },
        )

        response = test_api_client.patch(
            f'{self.base_route}/{create_success_response_dict["_id"]}',
            json={"feature_id": str(worse_readiness_feature_id)},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["user_id"] == create_success_response_dict["user_id"]
        assert response_dict["feature_ids"] == [
            str(feature_ids_before[0]),
            str(feature_id),
            str(worse_readiness_feature_id),
        ]
        assert response_dict["readiness"] == FeatureReadiness.DRAFT
        assert response_dict["default_feature_id"] == str(feature_id)
        assert response_dict["default_version_mode"] == DefaultVersionMode.AUTO

        # test update default version model
        response = test_api_client.patch(
            f'{self.base_route}/{create_success_response_dict["_id"]}',
            json={"default_version_mode": DefaultVersionMode.MANUAL},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["user_id"] == create_success_response_dict["user_id"]
        assert response_dict["feature_ids"] == [
            str(feature_ids_before[0]),
            str(feature_id),
            str(worse_readiness_feature_id),
        ]
        assert response_dict["readiness"] == FeatureReadiness.DRAFT
        assert response_dict["default_feature_id"] == str(feature_id)
        assert response_dict["default_version_mode"] == DefaultVersionMode.MANUAL

    def test_update_404(self, test_api_client_persistent):
        """
        Test update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{unknown_id}", json={})
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {"detail": f'FeatureNamespace (id: "{unknown_id}") not found.'}

    @pytest.mark.asyncio
    async def test_update_422(self, test_api_client_persistent, create_success_response):
        """
        Test update (unprocessable)
        """
        test_api_client, persistent = test_api_client_persistent
        create_success_response_dict = create_success_response.json()

        # insert a feature_id to feature collection
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document={
                "_id": ObjectId(),
                "user_id": ObjectId(create_success_response_dict["user_id"]),
                "name": "other_name",
                "readiness": FeatureReadiness.DRAFT.value,
            },
        )

        response = test_api_client.patch(
            f'{self.base_route}/{create_success_response_dict["_id"]}',
            json={"feature_id": str(feature_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": (
                'Feature (name: "other_name") object(s) within the same namespace '
                'must have the same "name" value (namespace: "sum_30m", version: '
                '"other_name").'
            )
        }
