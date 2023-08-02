"""
Test Feature Job Setting Analysis worker task
"""
import copy
import json
from unittest.mock import call

import pytest
from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import DuplicateDocumentError
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

    async def setup_persistent_storage(self, persistent, storage, temp_storage, catalog):
        """
        Setup for post route
        """
        # save feature store if it doesn't already exist
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        try:
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

    @pytest.fixture(autouse=True)
    def use_mock_event_dataset(self, mock_event_dataset):
        """
        Patch event dataset to skip calls to data warehouse
        """
        _ = mock_event_dataset
        yield

    async def _check_execution_result(
        self, payload, output_document_id, persistent, storage, progress, update_fixtures
    ):
        # check that analysis result is stored in persistent
        document = await persistent.find_one(
            collection_name=FeatureJobSettingAnalysisModel.collection_name(),
            query_filter={"_id": ObjectId(output_document_id)},
            user_id=None,
        )
        assert document
        result = FeatureJobSettingAnalysisModel(
            **json.loads(FeatureJobSettingAnalysisModel(**document).json())
        )

        # check document output
        fixture_path = "tests/fixtures/feature_job_setting_analysis/result.json"
        if update_fixtures:
            with open(fixture_path, "w") as file_obj:
                json.dump(result.json_dict(), file_obj, indent=4)

        persistent_fixture = BaseTaskTestSuite.load_payload(fixture_path)
        expected = FeatureJobSettingAnalysisModel(**persistent_fixture)

        payload = FeatureJobSettingAnalysisTask.payload_class(**self.payload)
        assert result.user_id == payload.user_id
        assert result.analysis_options == expected.analysis_options
        assert result.analysis_parameters == expected.analysis_parameters
        assert result.analysis_result == expected.analysis_result
        assert result.analysis_report == expected.analysis_report
        if result.event_table_id:
            assert result.event_table_id == expected.event_table_id
            assert result.event_table_candidate is None
        else:
            assert result.event_table_id is None
            assert result.event_table_candidate == payload.event_table_candidate

        # check storage of large objects
        analysis_data = await storage.get_object(
            f"feature_job_setting_analysis/{output_document_id}/data.json"
        )
        assert sorted(analysis_data.keys()) == [
            "analysis_data",
            "analysis_plots",
            "analysis_result",
        ]

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
            call({"percent": 5, "message": "Running Analysis"}),
            call({"percent": 95, "message": "Saving Analysis"}),
            call({"percent": 100, "message": "Analysis Completed"}),
        ]

    @pytest.mark.asyncio
    async def test_execute_success(  # pylint: disable=too-many-locals
        self, task_completed, mongo_persistent, progress, update_fixtures, storage
    ):
        """
        Test successful task execution
        """
        _ = task_completed
        persistent, _ = mongo_persistent
        output_document_id = self.payload["output_document_id"]
        await self._check_execution_result(
            payload=self.payload,
            output_document_id=output_document_id,
            persistent=persistent,
            storage=storage,
            progress=progress,
            update_fixtures=update_fixtures,
        )

    @pytest.mark.asyncio
    async def test_execute_fail(
        self, mongo_persistent, progress, storage, temp_storage, get_credential
    ):
        """
        Test failed task execution
        """
        persistent, _ = mongo_persistent

        # execute task with payload
        event_table_id = ObjectId()
        payload = copy.deepcopy(self.payload)
        payload["event_table_id"] = event_table_id
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
        assert (
            str(excinfo.value)
            == f'EventTable (id: "{event_table_id}") not found. Please save the EventTable object first.'
        )

        # check progress update records
        assert progress.put.call_args_list == [
            call({"percent": 0, "message": "Preparing data"}),
        ]

    @pytest.mark.asyncio
    async def test_execute_without_event_table_success(  # pylint: disable=too-many-locals
        self,
        catalog,
        mongo_persistent,
        progress,
        update_fixtures,
        storage,
        temp_storage,
        get_credential,
    ):
        """
        Test successful task execution without using existing event table
        """
        persistent, _ = mongo_persistent
        event_table_payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")

        # execute task
        payload = copy.deepcopy(self.payload)
        payload.pop("event_table_id", None)
        payload["catalog_id"] = catalog.id
        payload["event_table_candidate"] = {
            "name": event_table_payload["name"],
            "tabular_source": event_table_payload["tabular_source"],
            "event_timestamp_column": event_table_payload["event_timestamp_column"],
            "record_creation_timestamp_column": event_table_payload[
                "record_creation_timestamp_column"
            ],
        }
        await self.execute_task(
            task_class=self.task_class,
            payload=payload,
            persistent=persistent,
            progress=progress,
            storage=storage,
            temp_storage=temp_storage,
            get_credential=get_credential,
        )

        output_document_id = payload["output_document_id"]
        await self._check_execution_result(
            payload=payload,
            output_document_id=output_document_id,
            persistent=persistent,
            storage=storage,
            progress=progress,
            update_fixtures=update_fixtures,
        )
