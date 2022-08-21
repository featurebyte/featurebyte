"""
Test Feature Job Setting Analysis worker task
"""
import os

import pytest

from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisTaskPayload,
)
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

    @pytest.fixture(autouse=True)
    def mock_event_dataset(self):
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

    def test_task(self):
        """
        Test execution of the task
        """
        task = self.task_class(self.payload)
