"""
Test Feature Job Setting Analysis worker task
"""
import copy
import datetime
from unittest.mock import Mock, call
from uuid import uuid4

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
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

    async def setup_persistent_storage(self, persistent, storage, temp_storage, catalog):
        """
        Setup for post route
        """
        # save feature store
        try:
            payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
            await persistent.insert_one(
                collection_name=FeatureStoreModel.collection_name(),
                document=FeatureStoreModel(**payload).dict(by_alias=True),
                user_id=None,
            )
        except DuplicateDocumentError:
            # do nothing as it means this has been created before
            pass

        # save event table
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        payload["catalog_id"] = catalog.id
        await persistent.insert_one(
            collection_name=EventTableModel.collection_name(),
            document=EventTableModel(**payload).dict(by_alias=True),
            user_id=None,
        )

    @pytest_asyncio.fixture(autouse=True)
    async def setup(  # pylint: disable=W0221
        self, mongo_persistent, storage, temp_storage, mock_event_dataset, get_credential, catalog
    ):
        _ = mock_event_dataset
        persistent, _ = mongo_persistent
        await self.setup_persistent_storage(persistent, storage, temp_storage, catalog)

        # save analyse
        payload = self.load_payload(
            "tests/fixtures/task_payloads/feature_job_setting_analysis.json"
        )
        payload["catalog_id"] = catalog.id
        await self.execute_task(
            task_class=FeatureJobSettingAnalysisTask,
            payload=payload,
            persistent=persistent,
            progress=None,
            storage=storage,
            temp_storage=temp_storage,
            get_credential=get_credential,
        )

    @pytest.mark.asyncio
    async def test_execute_success(  # pylint: disable=too-many-locals
        self,
        mongo_persistent,
        task_completed,
        progress,
        temp_storage,
        update_fixtures,
    ):
        """
        Test successful task execution
        """
        _ = task_completed
        persistent, _ = mongo_persistent
        output_document_id = self.payload["output_document_id"]

        # check storage of results in temp storage
        prefix = f"feature_job_setting_analysis/backtest/{output_document_id}"
        analysis_plot = await temp_storage.get_text(f"{prefix}.html")
        analysis_data = await temp_storage.get_dataframe(f"{prefix}.parquet")

        results_fixture_path = "tests/fixtures/feature_job_setting_analysis/backtest.parquet"
        if update_fixtures:
            analysis_data.to_parquet(results_fixture_path)
        else:
            # check analysis table
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
            call({"percent": 0, "message": "Preparing table"}),
            call({"percent": 5, "message": "Running Analysis"}),
            call({"percent": 95, "message": "Saving Analysis"}),
            call({"percent": 100, "message": "Analysis Completed"}),
        ]

        # check backtest summary is saved in analysis document
        analysis_document = await persistent.find_one(
            collection_name="feature_job_setting_analysis",
            query_filter={"_id": ObjectId(self.payload["feature_job_setting_analysis_id"])},
        )
        backtest_summary = analysis_document["backtest_summaries"][0]
        assert isinstance(backtest_summary.pop("created_at"), datetime.datetime)
        assert backtest_summary == {
            "user_id": ObjectId(self.payload["user_id"]),
            "output_document_id": ObjectId(output_document_id),
            "feature_job_setting": {
                "frequency": 180,
                "job_time_modulo_frequency": 61,
                "blind_spot": 10,
                "feature_cutoff_modulo_frequency": 0,
            },
            "pct_incomplete_jobs": 100.0,
            "total_pct_late_data": 29.598486441034268,
        }

    @pytest.mark.asyncio
    async def test_execute_fail(
        self, mongo_persistent, progress, storage, temp_storage, get_credential
    ):
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
                get_credential=get_credential,
            )
        assert str(excinfo.value) == (
            f'FeatureJobSettingAnalysis (id: "{feature_job_setting_analysis_id}") not found. '
            "Please save the FeatureJobSettingAnalysis object first."
        )

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing table"}),
        ]

    @pytest.mark.asyncio
    async def test_get_task_description(self, persistent, catalog):
        """
        Test get task description
        """
        payload = FeatureJobSettingAnalysisBacktestTask.payload_class(**self.payload)
        task = FeatureJobSettingAnalysisBacktestTask(
            task_id=uuid4(),
            payload=payload.dict(by_alias=True),
            progress=Mock(),
            get_credential=Mock(),
            app_container=LazyAppContainer(
                user=Mock(),
                persistent=persistent,
                temp_storage=Mock(),
                celery=Mock(),
                redis=Mock(),
                storage=Mock(),
                catalog_id=catalog.id,
                app_container_config=app_container_config,
            ),
        )
        assert (
            await task.get_task_description()
            == 'Backtest feature job settings for table "sf_event_table"'
        )
