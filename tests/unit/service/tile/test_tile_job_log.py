"""
Unit tests for TileJobLogService
"""
from datetime import datetime

import freezegun
import pandas as pd
import pytest
import pytest_asyncio

from featurebyte.models.tile import TileType
from featurebyte.models.tile_job_log import TileJobLogModel


@pytest.fixture(name="tile_job_log_models")
def tile_job_logs_models_fixture():
    """
    Fixture for tile job log models
    """
    return [
        TileJobLogModel(
            tile_id="tile_id_1",
            aggregation_id="agg_id_1",
            tile_type=TileType.ONLINE,
            session_id="session_id_1",
            status="STARTED",
            message="good",
            created_at=datetime(2023, 1, 15, 10, 0, 0),
        ),
        TileJobLogModel(
            tile_id="tile_id_1",
            aggregation_id="agg_id_1",
            tile_type=TileType.ONLINE,
            session_id="session_id_1",
            status="FAILED",
            message="bad",
            created_at=datetime(2023, 1, 15, 12, 0, 0),
        ),
        TileJobLogModel(
            tile_id="tile_id_1",
            aggregation_id="agg_id_2",
            tile_type=TileType.ONLINE,
            session_id="session_id_2",
            status="COMPLETED",
            message="",
            created_at=datetime(2023, 1, 15, 12, 15, 0),
        ),
        TileJobLogModel(
            tile_id="tile_id_1",
            aggregation_id="agg_id_1",
            tile_type=TileType.OFFLINE,
            session_id="session_id_1",
            status="MONITORED",
            message="good",
            created_at=datetime(2023, 1, 15, 12, 0, 0),
        ),
    ]


@pytest_asyncio.fixture(name="saved_models")
async def saved_models_fixture(tile_job_log_service, tile_job_log_models):
    """
    Fixture for saved tile job log models
    """
    out = []
    for model in tile_job_log_models:
        if model.created_at is not None:
            created_at = model.created_at
        else:
            created_at = datetime.utcnow()
        with freezegun.freeze_time(created_at):
            out.append(await tile_job_log_service.create_document(model))
    return out


@freezegun.freeze_time(datetime(2023, 1, 15, 12, 30, 0))
@pytest.mark.usefixtures("saved_models")
@pytest.mark.asyncio
async def test_get_jobs_dataframe_default_hour_limit(tile_job_log_service):
    """
    Test get_jobs_dataframe with default hour limit
    """
    df = await tile_job_log_service.get_logs_dataframe(["agg_id_1"], 1)
    df.sort_values("created_at", inplace=True)
    df = df.reset_index(drop=True)
    expected_df = pd.DataFrame(
        {
            "session_id": ["session_id_1"],
            "created_at": [datetime(2023, 1, 15, 12, 0, 0)],
            "aggregation_id": ["agg_id_1"],
            "status": ["FAILED"],
            "message": ["bad"],
        }
    )
    pd.testing.assert_frame_equal(df, expected_df)


@freezegun.freeze_time(datetime(2023, 1, 15, 12, 30, 0))
@pytest.mark.usefixtures("saved_models")
@pytest.mark.asyncio
async def test_get_jobs_dataframe_custom_hour_limit(tile_job_log_service):
    """
    Test get_jobs_dataframe with custom hour limit
    """
    df = await tile_job_log_service.get_logs_dataframe(["agg_id_1"], 3)
    df.sort_values("created_at", inplace=True)
    df = df.reset_index(drop=True)
    expected_df = pd.DataFrame(
        {
            "session_id": ["session_id_1"] * 2,
            "created_at": [datetime(2023, 1, 15, 10, 0, 0), datetime(2023, 1, 15, 12, 0, 0)],
            "aggregation_id": ["agg_id_1"] * 2,
            "status": ["STARTED", "FAILED"],
            "message": ["good", "bad"],
        }
    )
    pd.testing.assert_frame_equal(df, expected_df)


@freezegun.freeze_time(datetime(2023, 1, 15, 12, 30, 0))
@pytest.mark.usefixtures("saved_models")
@pytest.mark.asyncio
async def test_get_jobs_dataframe_multiple_aggregation_ids(tile_job_log_service):
    """
    Test get_jobs_dataframe with multiple aggregation ids
    """
    df = await tile_job_log_service.get_logs_dataframe(["agg_id_1", "agg_id_2"], 1)
    df.sort_values("created_at", inplace=True)
    df = df.reset_index(drop=True)
    expected_df = pd.DataFrame(
        {
            "session_id": ["session_id_1", "session_id_2"],
            "created_at": [datetime(2023, 1, 15, 12, 0, 0), datetime(2023, 1, 15, 12, 15, 0)],
            "aggregation_id": ["agg_id_1", "agg_id_2"],
            "status": ["FAILED", "COMPLETED"],
            "message": ["bad", ""],
        }
    )
    pd.testing.assert_frame_equal(df, expected_df)
