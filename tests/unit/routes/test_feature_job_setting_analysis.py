"""
Tests for FeatureJobSettingAnalysis routes
"""

import copy
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
                    "ctx": {"ge": 3600},
                    "input": 0,
                    "loc": ["body", "analysis_length"],
                    "msg": "Input should be greater than or equal to 3600",
                    "type": "greater_than_equal",
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
                results=pd.DataFrame({
                    "count_on_time": [1, 2, 3],
                    "total_count": [3, 5, 6],
                    "pct_late_data": [0.33, 0.4, 0.5],
                }),
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
            "featurebyte.worker.task.feature_job_setting_analysis_backtest.FeatureJobSettingsAnalysisResult.from_dict",
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

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("event_table", "event_table"),
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
                "period": "180s",
                "offset": "61s",
                "execution_buffer": "0s",
            },
            "catalog_name": "grocery",
        }
        assert response_dict.items() == expected_info_response.items()

    @pytest.mark.asyncio
    async def test_list_records_with_warehouse_info(
        self, test_api_client_persistent, create_success_response
    ):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK, response.text

        wh_record_fields = [
            "job_frequency",
            "job_interval",
            "job_time_modulo_frequency",
            "jobs_count",
            "missing_jobs_count",
        ]
        warehouse_info = response.json()["data"][0]["stats_on_wh_jobs"]
        warehouse_record = {field: warehouse_info[field] for field in wh_record_fields}

        expected_warehouse_record = {
            "job_frequency": {"best_estimate": 180, "confidence": "high"},
            "job_interval": {
                "avg": 180.16713693942486,
                "max": 541.0,
                "median": 180.0,
                "min": 128.0,
            },
            "job_time_modulo_frequency": {
                "ends": 55,
                "ends_wo_late": 5,
                "job_at_end_of_cycle": False,
                "starts": 2,
            },
            "jobs_count": 3354,
            "missing_jobs_count": 5,
        }
        assert warehouse_record == expected_warehouse_record

    @pytest.mark.asyncio
    async def test_create_no_event_table_id(self, test_api_client_persistent):
        """
        Create request for event table with no event_table_id
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = self.payload.copy()
        payload.pop("event_table_id", None)
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "input": payload,
                "ctx": {"error": {}},
                "loc": ["body"],
                "msg": "Value error, Either event_table_id or event_table_candidate is required",
                "type": "value_error",
            }
        ]

    @pytest.mark.asyncio
    async def test_create_event_table_candidate(self, test_api_client_persistent):
        """
        Create request for event table with event_table_candidate
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        event_table_payload = BaseAsyncApiTestSuite.load_payload(
            "tests/fixtures/request_payloads/event_table.json"
        )
        payload = self.payload.copy()
        payload.pop("event_table_id", None)
        payload["event_table_candidate"] = {
            "name": event_table_payload["name"],
            "tabular_source": event_table_payload["tabular_source"],
            "event_timestamp_column": event_table_payload["event_timestamp_column"],
            "record_creation_timestamp_column": event_table_payload[
                "record_creation_timestamp_column"
            ],
        }
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.CREATED

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """
        Delete existing analysis
        """
        test_api_client, _ = test_api_client_persistent
        analysis_id = create_success_response.json()["_id"]
        with patch("featurebyte.storage.base.Storage.try_delete_if_exists") as mock_delete_file:
            response = test_api_client.delete(f"{self.base_route}/{analysis_id}")
            assert response.status_code == HTTPStatus.OK, response.json()

        mock_call_args = mock_delete_file.call_args
        assert mock_delete_file.call_count == 1
        assert str(mock_call_args[0][0]).startswith(
            f"feature_job_setting_analysis/{analysis_id}/data.json"
        )

        # check analysis is no longer available
        response = test_api_client.get(f"{self.base_route}/{analysis_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_event_table(self, test_api_client_persistent, create_success_response):
        """
        Delete event table
        """
        create_response_dict = create_success_response.json()
        event_table_id = create_response_dict["event_table_id"]
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"/event_table/{event_table_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "EventTable is referenced by FeatureJobSettingAnalysis: sample_analysis"
        )

        # delete the analysis and then delete the event table
        analysis_id = create_response_dict["_id"]
        response = test_api_client.delete(f"{self.base_route}/{analysis_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.delete(f"/event_table/{event_table_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
