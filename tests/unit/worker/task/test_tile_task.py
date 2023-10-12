"""
Test target table task
"""

import pytest
from bson import ObjectId

from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.worker.task.tile_task import TileTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container: LazyAppContainer):
    """
    Test get task description
    """
    payload = TileTaskPayload(
        name="Test Target Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        parameters=TileScheduledJobParameters(
            offline_period_minute=3600 * 24,
            tile_type="ONLINE",
            monitor_periods=10,
            feature_store_id=ObjectId(),
            tile_id="tile_id",
            aggregation_id="aggregation_id",
            time_modulo_frequency_second=0,
            blind_spot_second=60,
            frequency_minute=3600,
            sql="some sql",
            entity_column_names=[],
            value_column_names=[],
            value_column_types=[],
        ),
    )
    task = app_container.get(TileTask)
    assert await task.get_task_description(payload) == 'Generate tile for "tile_id:aggregation_id"'
