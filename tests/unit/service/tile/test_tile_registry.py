"""
Unit tests for TileRegistryService
"""
from datetime import datetime

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.tile import TileType
from featurebyte.models.tile_registry import TileModel


@pytest.fixture(name="tile_model")
def tile_model_fixture() -> TileModel:
    """
    Fixture for a tile model
    """
    return TileModel(
        tile_id="some_tile_id",
        aggregation_id="some_agg_id",
        tile_sql="SELECT * FROM tab",
        entity_column_names=["entity1"],
        value_column_names=["value1", "value2"],
        value_column_types=["FLOAT", "FLOAT"],
        frequency_minute=60,
        time_modulo_frequency_second=0,
        blind_spot_second=30,
        tile_type=TileType.ONLINE,
        tile_index=10000,
        tile_start_date=datetime(2021, 1, 1),
        feature_store_id=ObjectId(),
    )


@pytest_asyncio.fixture(name="saved_tile_model")
async def saved_tile_model_fixture(tile_registry_service, tile_model) -> TileModel:
    """
    Fixture for a saved tile model
    """
    return await tile_registry_service.create_document(tile_model)


@pytest.mark.asyncio
async def test_get_tile_model(tile_registry_service, saved_tile_model):
    """
    Test TileRegistryService.get_tile_model
    """
    retrieved_tile_model = await tile_registry_service.get_tile_model("some_tile_id", "some_agg_id")
    assert retrieved_tile_model is not None
    assert retrieved_tile_model.dict() == saved_tile_model.dict()


@pytest.mark.parametrize("tile_type", [TileType.ONLINE, TileType.OFFLINE])
@pytest.mark.asyncio
async def test_update_last_run_metadata(tile_registry_service, saved_tile_model, tile_type):
    """
    Test TileRegistryService.update_last_run_metadata
    """
    await tile_registry_service.update_last_run_metadata(
        tile_id=saved_tile_model.tile_id,
        aggregation_id=saved_tile_model.aggregation_id,
        tile_type=tile_type,
        tile_index=10005,
        tile_end_date=datetime(2021, 1, 2),
    )
    retrieved_tile_model = await tile_registry_service.get_tile_model(
        saved_tile_model.tile_id, saved_tile_model.aggregation_id
    )

    if tile_type == TileType.ONLINE:
        assert retrieved_tile_model.last_run_metadata_online.index == 10005
        assert retrieved_tile_model.last_run_metadata_online.tile_end_date == datetime(2021, 1, 2)
        assert retrieved_tile_model.last_run_metadata_offline is None
    else:
        assert retrieved_tile_model.last_run_metadata_online is None
        assert retrieved_tile_model.last_run_metadata_offline.index == 10005
        assert retrieved_tile_model.last_run_metadata_offline.tile_end_date == datetime(2021, 1, 2)


@pytest.mark.asyncio
async def test_update_last_run_metadata__both_tile_type(tile_registry_service, saved_tile_model):
    """
    Test multiple calls to TileRegistryService.update_last_run_metadata
    """
    await tile_registry_service.update_last_run_metadata(
        tile_id=saved_tile_model.tile_id,
        aggregation_id=saved_tile_model.aggregation_id,
        tile_type=TileType.ONLINE,
        tile_index=10005,
        tile_end_date=datetime(2021, 1, 2),
    )
    await tile_registry_service.update_last_run_metadata(
        tile_id=saved_tile_model.tile_id,
        aggregation_id=saved_tile_model.aggregation_id,
        tile_type=TileType.OFFLINE,
        tile_index=10010,
        tile_end_date=datetime(2021, 1, 5),
    )
    retrieved_tile_model = await tile_registry_service.get_tile_model(
        saved_tile_model.tile_id, saved_tile_model.aggregation_id
    )
    assert retrieved_tile_model.last_run_metadata_online.index == 10005
    assert retrieved_tile_model.last_run_metadata_online.tile_end_date == datetime(2021, 1, 2)
    assert retrieved_tile_model.last_run_metadata_offline.index == 10010
    assert retrieved_tile_model.last_run_metadata_offline.tile_end_date == datetime(2021, 1, 5)


@pytest.mark.asyncio
async def test_update_last_run_metadata__not_found(tile_registry_service, saved_tile_model):
    """
    Test TileRegistryService.update_last_run_metadata
    """
    with pytest.raises(DocumentNotFoundError) as exc_info:
        await tile_registry_service.update_last_run_metadata(
            tile_id="non_existing_tile_id",
            aggregation_id=saved_tile_model.aggregation_id,
            tile_type=TileType.ONLINE,
            tile_index=10005,
            tile_end_date=datetime(2021, 1, 2),
        )
    assert (
        str(exc_info.value)
        == "TileRegistryService: TileModel with tile_id=non_existing_tile_id and aggregation_id=some_agg_id not found"
    )
