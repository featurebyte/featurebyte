"""
Common fixtures for worker task tests
"""
import os
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest


@pytest.fixture(name="mock_event_dataset")
def mock_event_dataset():
    """
    Mock event dataset for feature job analysis to avoid pulling data from feature store
    """
    fixture_path = "tests/fixtures/feature_job_setting_analysis"
    count_data = pd.read_parquet(os.path.join(fixture_path, "count_data.parquet"))
    count_per_creation_date = pd.read_parquet(
        os.path.join(fixture_path, "count_per_creation_date.parquet")
    )
    count_per_creation_date["CREATION_DATE"] = pd.to_datetime(
        count_per_creation_date["CREATION_DATE"]
    )

    mock_targets = [
        (
            "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_unique_creation_date_ratio",
            lambda analysis_date, analysis_start: 100,
        ),
        (
            "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_latest_timestamp",
            lambda: pd.Timestamp("2022-04-18 23:59:55.799897854"),
        ),
        (
            "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_count_per_creation_date",
            lambda analysis_date, analysis_start: count_per_creation_date,
        ),
        (
            "featurebyte_freeware.feature_job_analysis.database.EventDataset.get_count_data",
            lambda analysis_date, analysis_start, granularity_seconds, reading_at: count_data,
        ),
    ]
    mock_patches = {}
    for target, side_effect in mock_targets:
        mock_patch = patch(target, side_effect=side_effect, new_callable=AsyncMock)
        mock_patch.start()
        mock_patches[target] = mock_patch

    yield

    for mock_patch in mock_patches.values():
        mock_patch.stop()
