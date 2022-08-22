"""
Tests for FeatureJobSettingAnalysis routes
"""
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson import ObjectId

from tests.unit.routes.base import BaseAsyncApiTestSuite


class TestFeatureJobSettingAnalysisApi(BaseAsyncApiTestSuite):
    """
    TestFeatureJobSettingAnalysisApi class
    """

    class_name = "FeatureJobSettingAnalysis"
    base_route = "/feature_job_setting_analysis"
    payload = BaseAsyncApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_job_setting_analysis.json"
    )

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureJobSettingAnalysis (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `FeatureJobSettingAnalysis.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]

    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "analysis_length": 0},
            [
                {
                    "loc": ["body", "analysis_length"],
                    "msg": "ensure this value is greater than or equal to 3600",
                    "type": "value_error.number.not_ge",
                    "ctx": {"limit_value": 3600},
                }
            ],
        )
    ]

    @pytest.fixture(autouse=True)
    def mock_analysis(self):
        """
        Apply patch on call to analysis
        """
        result = self.load_payload("tests/fixtures/feature_job_setting_analysis/result.json")
        record = Mock(**result, to_html=lambda: result["analysis_report"])
        with patch(
            "featurebyte.worker.task.feature_job_setting_analysis.create_feature_job_settings_analysis",
        ) as mock_create_feature_job_settings_analysis:
            mock_create_feature_job_settings_analysis.return_value = record
            yield

    @pytest.fixture(autouse=True)
    def mock_snowflake(self, snowflake_connector, snowflake_execute_query):
        """
        Apply patch on snowflake operations
        """
        yield

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        # save feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        response = api_client.post("/feature_store", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        # save event data
        payload = self.load_payload("tests/fixtures/request_payloads/event_data.json")
        response = api_client.post("/event_data", json=payload)
        assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload

    def test_create_event_data_not_found(self, test_api_client_persistent):
        """
        Create request for non-existent event data
        """
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["event_data_id"] = str(ObjectId("63030c9eb9150a577ebb61fb"))
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert (
            response.json()["detail"]
            == 'EventData (id: "63030c9eb9150a577ebb61fb") not found. Please save the EventData object first.'
        )
