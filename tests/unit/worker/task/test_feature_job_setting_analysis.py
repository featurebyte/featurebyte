"""
Test Feature Job Setting Analysis worker task
"""

import copy
import json
from unittest.mock import call, patch

import bs4
import pytest
from bson import ObjectId

from featurebyte import DatabricksDetails, SourceType
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.routes.lazy_app_container import LazyAppContainer
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
                document=FeatureStoreModel(**payload).model_dump(by_alias=True),
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
            document=EventTableModel(**payload).model_dump(by_alias=True),
            user_id=None,
        )

    @pytest.fixture(autouse=True)
    def insert_credential_fixture(self, insert_credential):
        """
        Insert default credential into db.
        """
        _ = insert_credential
        yield

    @pytest.fixture(autouse=True)
    def use_mock_event_dataset(self, mock_event_dataset):
        """
        Patch event dataset to skip calls to data warehouse
        """
        _ = mock_event_dataset
        yield

    @staticmethod
    def clean_html(html):
        """
        Strip img tag from html
        """
        soup = bs4.BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("img"):
            # strip image tag with base64 encoded image to avoid issue where different OS may
            # generate different base64 encoded image (could be due to different font rendering)
            if tag["src"].startswith("data:image/png;base64"):
                tag.decompose()
        return str(soup)

    async def _check_execution_result(
        self, payload, output_document_id, persistent, storage, progress, update_fixtures
    ):
        # check that analysis result is stored in persistent
        document = await persistent.find_one(
            collection_name=FeatureJobSettingAnalysisModel.collection_name(),
            query_filter={"_id": ObjectId(output_document_id)},
        )
        assert document
        result = FeatureJobSettingAnalysisModel(
            **json.loads(FeatureJobSettingAnalysisModel(**document).model_dump_json())
        )

        # check document output
        if result.event_table_id:
            fixture_path = "tests/fixtures/feature_job_setting_analysis/result.json"
        else:
            fixture_path = "tests/fixtures/feature_job_setting_analysis/result_no_event_table.json"
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
        assert self.clean_html(result.analysis_report) == self.clean_html(expected.analysis_report)
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
        self, mongo_persistent, progress, storage, temp_storage, app_container
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
                app_container=app_container,
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
        app_container,
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
            app_container=app_container,
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

    @pytest.mark.asyncio
    async def test_get_task_description(self, persistent, catalog, app_container: LazyAppContainer):
        """
        Test get task description
        """
        app_container.override_instance_for_test("catalog_id", catalog.id)
        app_container.override_instance_for_test("persistent", persistent)
        task = app_container.get(FeatureJobSettingAnalysisTask)
        payload = task.get_payload_obj(self.payload)
        assert (
            await task.get_task_description(payload)
            == 'Analyze feature job settings for table "sf_event_table"'
        )

    @pytest.mark.asyncio
    async def test_execute_with_databricks_store_success(  # pylint: disable=too-many-locals
        self,
        catalog,
        mongo_persistent,
        progress,
        update_fixtures,
        storage,
        temp_storage,
        app_container,
        snowflake_feature_store,
    ):
        """
        Test successful task execution using databricks feature store
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

        with (
            patch(
                "featurebyte.worker.task.feature_job_setting_analysis.FeatureStoreService.get_document"
            ) as mock_get_document,
            patch(
                "featurebyte.worker.task.feature_job_setting_analysis.SessionManagerService.get_feature_store_session"
            ),
        ):
            feature_store = FeatureStoreModel(**snowflake_feature_store.model_dump(by_alias=True))
            mock_get_document.return_value = feature_store
            feature_store.details = DatabricksDetails(
                host="hostname",
                http_path="http_path",
                catalog_name="spark_catalog",
                schema_name="featurebyte",
                storage_path="dbfs:/FileStore/featurebyte",
            )
            feature_store.type = SourceType.DATABRICKS

            await self.execute_task(
                task_class=self.task_class,
                payload=payload,
                persistent=persistent,
                progress=progress,
                storage=storage,
                temp_storage=temp_storage,
                app_container=app_container,
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
