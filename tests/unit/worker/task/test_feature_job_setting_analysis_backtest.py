"""
Test Feature Job Setting Analysis worker task
"""
import copy
from unittest.mock import call

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktestTask,
    FeatureJobSettingAnalysisTask,
)
from tests.unit.worker.task.base import BaseTaskTestSuite


class TestFeatureJobSettingAnalysisBacktestTask(BaseTaskTestSuite):
    """
    Test suite for Feature Job Setting Analysis worker task
    """

    task_class = FeatureJobSettingAnalysisBacktestTask
    payload = BaseTaskTestSuite.load_payload(
        "tests/fixtures/task_payloads/feature_job_setting_analysis_backtest.json"
    )

    async def setup_persistent_storage(self, persistent, storage, temp_storage):
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
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        await persistent.insert_one(
            collection_name=EventTableModel.collection_name(),
            document=EventTableModel(**payload).dict(by_alias=True),
            user_id=None,
        )

        # save analyse
        payload = self.load_payload(
            "tests/fixtures/task_payloads/feature_job_setting_analysis.json"
        )
        await self.execute_task(
            task_class=FeatureJobSettingAnalysisTask,
            payload=payload,
            persistent=persistent,
            progress=None,
            storage=storage,
            temp_storage=temp_storage,
        )

    @pytest_asyncio.fixture(autouse=True)
    async def setup(  # pylint: disable=W0221
        self, mongo_persistent, storage, temp_storage, mock_event_dataset
    ):
        _ = mock_event_dataset
        persistent, _ = mongo_persistent
        await self.setup_persistent_storage(persistent, storage, temp_storage)

    @pytest.mark.asyncio
    async def test_execute_success(  # pylint: disable=too-many-locals
        self, task_completed, progress, temp_storage, update_fixtures
    ):
        """
        Test successful task execution
        """
        _ = task_completed
        output_document_id = self.payload["output_document_id"]

        # check storage of results in temp storage
        prefix = f"feature_job_setting_analysis/backtest/{output_document_id}"
        analysis_plot = await temp_storage.get_text(f"{prefix}.html")
        analysis_data = await temp_storage.get_dataframe(f"{prefix}.parquet")

        results_fixture_path = "tests/fixtures/feature_job_setting_analysis/backtest.parquet"
        if update_fixtures:
            analysis_data.to_parquet(results_fixture_path)
        else:
            # check analysis data
            expected_data = pd.read_parquet(results_fixture_path)
            assert_frame_equal(analysis_data, expected_data)

        report_fixture_path = "tests/fixtures/feature_job_setting_analysis/backtest.html"
        if update_fixtures:
            with open(report_fixture_path, "w") as file_obj:
                file_obj.write(analysis_plot)
        else:
            # check report
            with open(report_fixture_path, "r") as file_obj:
                expected_plot = file_obj.read()
            assert analysis_plot == expected_plot

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
            call({"percent": 5, "message": "Running Analysis"}),
            call({"percent": 95, "message": "Saving Analysis"}),
            call({"percent": 100, "message": "Analysis Completed"}),
        ]

    @pytest.mark.asyncio
    async def test_execute_fail(self, mongo_persistent, progress, storage, temp_storage):
        """
        Test failed task execution
        """
        persistent, _ = mongo_persistent

        # execute task with payload
        feature_job_setting_analysis_id = ObjectId()
        payload = copy.deepcopy(self.payload)
        payload["feature_job_setting_analysis_id"] = feature_job_setting_analysis_id

        with pytest.raises(DocumentNotFoundError) as excinfo:
            await self.execute_task(
                task_class=self.task_class,
                payload=payload,
                persistent=persistent,
                progress=progress,
                storage=storage,
                temp_storage=temp_storage,
            )
        assert str(excinfo.value) == (
            f'FeatureJobSettingAnalysis (id: "{feature_job_setting_analysis_id}") not found. '
            "Please save the FeatureJobSettingAnalysis object first."
        )

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
        ]
