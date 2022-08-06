"""
Tests for FeatureList route
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureListApi(BaseApiTestSuite):
    """
    TestFeatureListApi class
    """

    class_name = "FeatureList"
    base_route = "/feature_list"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_single.json"
    )
    payload_multi = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_multi.json"
    )
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureList (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `FeatureList.get(name="sf_feature_list")`.',
        ),
        (
            {**payload, "_id": object_id},
            'FeatureList (name: "sf_feature_list") already exists. '
            'Get the existing object by `FeatureList.get(name="sf_feature_list")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": object_id,
                "name": "random_name",
                "features": [
                    {"id": object_id, "name": "some_feature", "version": "random_version"}
                ],
            },
            f'Feature (id: "{object_id}") not found. ' "Please save the Feature object first.",
        )
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.routes.feature.controller.FeatureController.insert_feature_registry"
        ) as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def mock_insert_feature_list_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch(
            "featurebyte.routes.feature_list.controller.FeatureListController.insert_feature_list_registry"
        ) as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_objects = ["feature_store", "event_data", "feature"]
        for api_object in api_objects:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{api_object}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED
