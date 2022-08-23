"""
Tests for FeatureJobSettingAnalysis routes
"""
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson import ObjectId
from featurebyte_freeware.feature_job_analysis.schema import (
    AnalysisResult,
    BacktestResult,
    BlindSpotSearchResult,
    EventLandingTimeResult,
)
from pandas.testing import assert_frame_equal

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
        record = Mock(
            **result,
            to_html=lambda: result["analysis_report"],
            analysis_plots=None,
            analysis_data=None,
        )
        record.analysis_result = AnalysisResult(
            **record.analysis_result,
            backtest_result=BacktestResult(
                results=pd.DataFrame(),
                plot=None,
                job_with_issues_count=0,
                warnings=[],
            ),
            blind_spot_search_result=BlindSpotSearchResult(
                pct_late_data=0.5,
                optimal_blind_spot=0,
                results=pd.DataFrame(),
                plot="",
                thresholds=[],
                warnings=[],
            ),
            event_landing_time_result=EventLandingTimeResult(
                results=pd.DataFrame(),
                plot="",
                thresholds=[],
                warnings=[],
            ),
        )
        with patch(
            "featurebyte.worker.task.feature_job_setting_analysis.create_feature_job_settings_analysis",
        ) as mock_create_feature_job_settings_analysis:
            mock_create_feature_job_settings_analysis.return_value = record
            yield

    @pytest.fixture(name="backtest_result")
    def backtest_result_fixture(self):
        """
        Backtest result fixture
        """
        return (
            BacktestResult(
                results=pd.DataFrame({"a": [1, 2, 3], "b": [pd.Timestamp(datetime.now())] * 3}),
                plot=None,
                job_with_issues_count=0,
                warnings=[],
            ),
            "html_report_contents",
        )

    @pytest.fixture(autouse=True)
    def mock_result(self, backtest_result):
        """
        Apply patch to analysis result class
        """
        with patch(
            "featurebyte.worker.task.feature_job_setting_analysis.FeatureJobSettingsAnalysisResult",
        ) as mock_analysis_result_cls:
            mock_analysis_result = mock_analysis_result_cls.return_value
            mock_analysis_result.backtest.return_value = backtest_result
            yield

    @pytest.fixture(autouse=True)
    def mock_snowflake(self, snowflake_connector, snowflake_execute_query):
        """
        Apply patch on snowflake operations
        """
        _ = snowflake_connector
        _ = snowflake_execute_query
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
        for _ in range(3):
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
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == 'EventData (id: "63030c9eb9150a577ebb61fb") not found. Please save the EventData object first.'
        )

    @pytest.mark.asyncio
    async def test_storage(self, create_success_response, storage):
        """
        Check data from analysis uploaded to storage
        """
        feature_job_setting_analysis_id = create_success_response.json()["_id"]

        # check results are stored to storage
        data = await storage.get_object(
            f"feature_job_setting_analysis/{feature_job_setting_analysis_id}/data.pkl"
        )
        assert data == {"analysis_plots": None, "analysis_data": None}

    @pytest.mark.asyncio
    async def test_backtest(
        self, test_api_client_persistent, create_success_response, temp_storage, backtest_result
    ):
        """
        Run backtest for existing analysis
        """
        feature_job_setting_analysis_id = create_success_response.json()["_id"]
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_job_settings_analysis_backtest.json"
        )
        response = test_api_client.post(
            f"{self.base_route}/{feature_job_setting_analysis_id}/backtest", json=payload
        )
        assert response.status_code == HTTPStatus.ACCEPTED
        output_document_id = response.json()["payload"]["output_document_id"]

        output_path = self.wait_for_results(test_api_client, response)

        # check results are stored to temp storage
        prefix = f"feature_job_setting_analysis/backtest/{output_document_id}"
        report_content = await temp_storage.get_text(f"{prefix}.html")
        assert report_content == backtest_result[1]

        backtest_dataframe = await temp_storage.get_dataframe(f"{prefix}.parquet")
        assert_frame_equal(backtest_dataframe, backtest_result[0].results)
