"""
Tests for FeatureJobSettingAnalysis routes
"""
import copy
from datetime import datetime
from http import HTTPStatus
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from bson import ObjectId
from featurebyte_freeware.feature_job_analysis.analysis import HighUpdateFrequencyError
from featurebyte_freeware.feature_job_analysis.schema import (
    AnalysisOptions,
    AnalysisParameters,
    AnalysisResult,
    BacktestResult,
    BlindSpotSearchResult,
    EventLandingTimeResult,
)
from pandas.testing import assert_frame_equal

from featurebyte.models.base import DEFAULT_CATALOG_ID
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
    async_create = True

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

    @pytest.fixture(name="analysis_result")
    def analysis_result_fixture(self):
        """
        AnalysisResult fixture
        """
        result = self.load_payload("tests/fixtures/feature_job_setting_analysis/result.json")
        data = result["analysis_result"]
        data["blind_spot_search_exc_missing_jobs_result"] = None
        data["backtest_result"] = BacktestResult(
            results=pd.DataFrame(),
            plot=None,
            job_with_issues_count=0,
            warnings=[],
        ).dict()
        data["blind_spot_search_result"] = BlindSpotSearchResult(
            pct_late_data=0.5,
            optimal_blind_spot=0,
            results=pd.DataFrame(),
            plot="",
            thresholds=[],
            warnings=[],
        ).dict()
        data["event_landing_time_result"] = EventLandingTimeResult(
            results=pd.DataFrame(),
            plot="",
            thresholds=[],
            warnings=[],
        ).dict()
        return AnalysisResult.from_dict(data)

    @pytest.fixture(autouse=True, name="mock_analysis")
    def mock_analysis(self, analysis_result):
        """
        Apply patch on call to analysis
        """
        result = self.load_payload("tests/fixtures/feature_job_setting_analysis/result.json")
        result["analysis_plots"] = None
        result["analysis_data"] = None
        result["analysis_result"] = analysis_result.dict()
        kwargs = copy.deepcopy(result)
        kwargs["analysis_parameters"] = AnalysisParameters.from_dict(kwargs["analysis_parameters"])
        kwargs["analysis_options"] = AnalysisOptions.from_dict(kwargs["analysis_options"])
        kwargs["analysis_result"] = analysis_result
        record = Mock(**kwargs, to_html=lambda: result["analysis_report"], dict=lambda: result)
        with patch(
            "featurebyte.worker.task.feature_job_setting_analysis.create_feature_job_settings_analysis",
            new_callable=AsyncMock,
        ) as mock_create_feature_job_settings_analysis:
            mock_create_feature_job_settings_analysis.return_value = record
            yield mock_create_feature_job_settings_analysis

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
            "featurebyte.worker.task.feature_job_setting_analysis.FeatureJobSettingsAnalysisResult.from_dict",
        ) as mock_analysis_result_from_dict:
            mock_analysis_result = mock_analysis_result_from_dict.return_value
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        # save feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        response = api_client.post(
            "/feature_store", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED

        # save event table
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        response = api_client.post(
            "/event_table", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_create_event_table_not_found(self, test_api_client_persistent):
        """
        Create request for non-existent event table
        """
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["event_table_id"] = str(ObjectId("63030c9eb9150a577ebb61fb"))
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == 'EventTable (id: "63030c9eb9150a577ebb61fb") not found. Please save the EventTable object first.'
        )

    @pytest.mark.asyncio
    async def test_storage(self, create_success_response, storage):
        """
        Check table from analysis uploaded to storage
        """
        response_dict = create_success_response.json()
        feature_job_setting_analysis_id = response_dict["_id"]

        # check results are stored to storage
        data = await storage.get_object(
            f"feature_job_setting_analysis/{feature_job_setting_analysis_id}/data.json"
        )
        assert data["analysis_plots"] is None
        assert data["analysis_data"] is None

    @pytest.mark.asyncio
    async def test_backtest(
        self, test_api_client_persistent, create_success_response, temp_storage, backtest_result
    ):
        """
        Run backtest for existing analysis
        """
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_job_settings_analysis_backtest.json"
        )
        response = test_api_client.post(f"{self.base_route}/backtest", json=payload)
        assert response.status_code == HTTPStatus.CREATED
        response_dict = response.json()
        output_document_id = response_dict["payload"]["output_document_id"]
        assert response_dict["output_path"] == (
            f"/temp_data?path=feature_job_setting_analysis/backtest/{output_document_id}"
        )

        self.wait_for_results(test_api_client, response)

        # check results are stored to temp storage
        prefix = f"feature_job_setting_analysis/backtest/{output_document_id}"
        report_content = await temp_storage.get_text(f"{prefix}.html")
        assert report_content == backtest_result[1]

        backtest_dataframe = await temp_storage.get_dataframe(f"{prefix}.parquet")
        assert_frame_equal(backtest_dataframe, backtest_result[0].results)

    @pytest.mark.asyncio
    async def test_create_event_table_no_creation_date(self, test_api_client_persistent):
        """
        Create request for event table with no creation date column
        """
        test_api_client, persistent = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # remove event table creation date column
        await persistent.update_one(
            collection_name="table",
            query_filter={},
            update={"$set": {"record_creation_timestamp_column": None}},
            user_id=None,
        )

        payload = self.payload.copy()
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == "Creation date column is not available for the event table."
        )

    @pytest.mark.asyncio
    async def test_create_event_table_high_frequency(
        self, mock_analysis, test_api_client_persistent
    ):
        """
        Create request for event table with overly high update frequency
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # simulate error raised during analysis
        mock_analysis.side_effect = HighUpdateFrequencyError
        response = test_api_client.post(f"{self.base_route}", json=self.payload)
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "FAILURE"
        assert "HighUpdateFrequencyError" in response_dict["traceback"]

    @pytest.mark.asyncio
    async def test_report_download(self, test_api_client_persistent, create_success_response):
        """
        Download pdf report download for existing analysis
        """
        response_dict = create_success_response.json()
        feature_job_setting_analysis_id = response_dict["_id"]
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(
            f"{self.base_route}/{feature_job_setting_analysis_id}/report"
        )
        assert response.status_code == HTTPStatus.OK
        assert len(response.content) > 0
        assert response.headers == {
            "content-disposition": (
                'attachment; name="report"; '
                'filename="feature_job_setting_analysis_62f301e841b73757c9ff879a.pdf"'
            ),
            "content-type": "application/pdf",
        }

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()

        expected_info_response = {
            "created_at": response_dict["created_at"],
            "event_table_name": "sf_event_table",
            "analysis_options": {
                "analysis_date": "2022-04-18T23:59:55.799000",
                "analysis_start": "2022-03-21T23:59:55.799000",
                "analysis_length": 2419200,
                "blind_spot_buffer_setting": 5,
                "exclude_late_job": False,
                "job_time_buffer_setting": "auto",
                "late_data_allowance": 5e-05,
                "min_featurejob_period": 60,
            },
            "recommendation": {
                "blind_spot": "395s",
                "frequency": "180s",
                "time_modulo_frequency": "61s",
            },
            "catalog_name": "default",
        }
        assert response_dict.items() == expected_info_response.items()
