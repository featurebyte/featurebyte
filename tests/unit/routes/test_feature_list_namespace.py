"""
Test for FeatureListNamespace route
"""
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureListNamespaceApi(BaseApiTestSuite):
    """
    TestFeatureListNamespaceApi
    """

    class_name = "FeatureListNamespace"
    base_route = "/feature_list_namespace"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'FeatureListNamespace (id: "{payload["_id"]}") already exists.')
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
        for _ in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload

    @pytest.mark.asyncio
    async def test_update_200(self, test_api_client_persistent, create_success_response):
        """
        Test update (success)
        """
        test_api_client, persistent = test_api_client_persistent
        feature_namespace_data_before_update, _ = await persistent.find(
            "feature_list_namespace", {}
        )
        create_success_response_dict = create_success_response.json()
        feature_list_ids_before = feature_namespace_data_before_update[0]["feature_list_ids"]
        assert len(feature_list_ids_before) == 1

        # insert a feature_list_id to feature collection
        feature_list_id = await persistent.insert_one(
            collection_name="feature_list",
            document={
                "_id": ObjectId(),
                "user_id": ObjectId(create_success_response_dict["user_id"]),
                "name": create_success_response_dict["name"],
                "readiness": FeatureReadiness.PRODUCTION_READY.value,
                "readiness_distribution": [{"readiness": "PRODUCTION_READY", "count": 2}],
            },
        )
        feature_list_data, _ = await persistent.find(
            collection_name="feature_list", query_filter={}
        )
        assert len(feature_list_data) == 1

        response = test_api_client.patch(
            f'{self.base_route}/{create_success_response_dict["_id"]}',
            json={"feature_list_id": str(feature_list_id)},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["user_id"] == create_success_response_dict["user_id"]
        assert response_dict["feature_list_ids"] == [
            str(feature_list_ids_before[0]),
            str(feature_list_id),
        ]
        assert response_dict["readiness"] == FeatureReadiness.PRODUCTION_READY
        assert response_dict["default_feature_list_id"] == str(feature_list_id)
        assert response_dict["default_version_mode"] == DefaultVersionMode.AUTO
