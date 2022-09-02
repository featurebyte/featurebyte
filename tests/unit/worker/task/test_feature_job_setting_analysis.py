"""
Test Feature Job Setting Analysis worker task
"""
import copy
import json
import os
from unittest.mock import call, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.worker.task.feature_job_setting_analysis import FeatureJobSettingAnalysisTask
from tests.unit.worker.task.base import BaseTaskTestSuite


class TestFeatureJobSettingAnalysisTask(BaseTaskTestSuite):
    """
    Test suite for Feature Job Setting Analysis worker task
    """

    task_class = FeatureJobSettingAnalysisTask
    payload = BaseTaskTestSuite.load_payload(
        "tests/fixtures/task_payloads/feature_job_setting_analysis.json"
    )

    async def setup_persistent(self, persistent):
        """
        Setup for post route
        """
        # save feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        await persistent.insert_one(
            collection_name=FeatureStoreModel.collection_name(),
            document=FeatureStoreModel(**payload).dict(by_alias=True),
            user_id=None,
        )

        # save event data
        payload = self.load_payload("tests/fixtures/request_payloads/event_data.json")
        await persistent.insert_one(
            collection_name=EventDataModel.collection_name(),
            document=EventDataModel(**payload).dict(by_alias=True),
            user_id=None,
        )

    @pytest.fixture(autouse=True)
    def mock_event_dataset(self):
        """
        Setup mock event dataset
        """
        fixture_path = "tests/fixtures/feature_job_setting_analysis"
        count_data = pd.read_parquet(os.path.join(fixture_path, "count_data.parquet"))
        count_per_creation_date = pd.read_parquet(
            os.path.join(fixture_path, "count_per_creation_date.parquet")
        )
        count_per_creation_date["CREATION_DATE"] = pd.to_datetime(
            count_per_creation_date["CREATION_DATE"]
        )

        with patch(
            "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_latest_timestamp",
        ) as mock_latest_timestamp:
            mock_latest_timestamp.return_value = pd.Timestamp("2022-04-18 23:59:55.799897854")
            with patch(
                "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_count_per_creation_date",
            ) as mock_get_count_per_creation_date:
                mock_get_count_per_creation_date.return_value = count_per_creation_date
                with patch(
                    "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_count_data",
                ) as mock_get_count_data:
                    mock_get_count_data.return_value = count_data
                    yield

    @pytest.mark.asyncio
    async def test_execute_success(self, task_completed, git_persistent, progress, update_fixtures):
        """
        Test successful task execution
        """
        _ = task_completed
        persistent, _ = git_persistent

        # check that analysis result is stored in persistent
        document = await persistent.find_one(
            collection_name=FeatureJobSettingAnalysisModel.collection_name(),
            query_filter={"_id": ObjectId(self.payload["output_document_id"])},
            user_id=None,
        )
        assert document
        result = FeatureJobSettingAnalysisModel(**document)

        # check document output
        fixture_path = "tests/fixtures/feature_job_setting_analysis/result.json"
        if update_fixtures:
            with open(fixture_path, "w") as file_obj:
                json.dump(result.json_dict(), file_obj, indent=4)

        persistent_fixture = BaseTaskTestSuite.load_payload(fixture_path)
        expected = FeatureJobSettingAnalysisModel(**persistent_fixture)

        payload = FeatureJobSettingAnalysisTask.payload_class(**self.payload)
        assert result.user_id == payload.user_id
        assert result.event_data_id == expected.event_data_id
        assert result.analysis_options == expected.analysis_options
        assert result.analysis_parameters == expected.analysis_parameters
        assert result.analysis_result == expected.analysis_result
        assert result.analysis_report == expected.analysis_report

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
            call({"percent": 5, "message": "Running Analysis"}),
            call({"percent": 95, "message": "Saving Analysis"}),
            call({"percent": 100, "message": "Analysis Completed"}),
        ]

    @pytest.mark.asyncio
    async def test_execute_fail(self, git_persistent, progress):
        """
        Test failed task execution
        """
        persistent, _ = git_persistent

        # execute task with payload
        payload = copy.deepcopy(self.payload)
        payload["event_data_id"] = ObjectId()

        with pytest.raises(ValueError) as excinfo:
            await self.execute_task(payload, persistent, progress)
        assert str(excinfo.value) == "Event Data not found"

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
        ]
