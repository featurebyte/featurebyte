"""
Tests for FeatureJobSettingAnalysis routes
"""
import datetime
import os
from http import HTTPStatus
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
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
        result = self.load_payload("tests/fixtures/feature_job_setting_analysis/result.json")
        record = Mock(**result, to_html=lambda: result["analysis_report"])
        with patch(
            "featurebyte.worker.task.feature_job_setting_analysis.create_feature_job_settings_analysis",
        ) as mock_create_feature_job_settings_analysis:
            mock_create_feature_job_settings_analysis.return_value = record
            yield

    @pytest.fixture(autouse=True)
    def mock_snowflake(self, persistent, snowflake_connector, snowflake_execute_query):
        yield

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        # save feature store
        feature_store_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_store.json"
        )
        response = api_client.post("/feature_store", json=feature_store_payload)
        assert response.status_code == HTTPStatus.CREATED

        # save event data
        feature_store_payload = self.load_payload("tests/fixtures/request_payloads/event_data.json")
        response = api_client.post("/event_data", json=feature_store_payload)
        assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload
