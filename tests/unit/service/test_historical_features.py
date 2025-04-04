"""
Test cases for the HistoricalFeaturesService
"""

from unittest.mock import AsyncMock, Mock, call, patch

import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte import CronFeatureJobSetting, Feature, FeatureList, exception
from featurebyte.models.system_metrics import TileComputeMetrics
from featurebyte.models.tile import OnDemandTileComputeResult, OnDemandTileTable
from featurebyte.models.warehouse_table import WarehouseTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.cron import JobScheduleTable, JobScheduleTableSet
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.service.historical_features import get_historical_features


@pytest.fixture
def mock_get_historical_features():
    """
    Mock the core compute_historical_features function that the service calls
    """
    with patch(
        "featurebyte.service.historical_features.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_historical_features = AsyncMock()
        with patch(
            "featurebyte.service.historical_features_and_target.get_historical_features",
            new=mock_get_historical_features,
        ):
            with patch(
                "featurebyte.service.historical_features.get_historical_features",
                new=mock_get_historical_features,
            ):
                mock_get_feature_store_session.return_value = Mock()
                yield mock_get_historical_features


@pytest.fixture(name="output_table_details")
def output_table_details_fixture():
    """Fixture for a TableDetails for the output location"""
    return TableDetails(table_name="SOME_HISTORICAL_FEATURE_TABLE")


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_not_deployed(
    historical_features_service,
    production_ready_feature_list,
    mock_get_historical_features,
    output_table_details,
):
    """
    Test compute_historical_features when feature list is not deployed
    """
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
        feature_list_id=production_ready_feature_list.id,
        feature_clusters=production_ready_feature_list.feature_clusters,
    )
    training_events = pd.DataFrame({"cust_id": [1], "POINT_IN_TIME": ["2022-01-01"]})

    await historical_features_service.compute(
        training_events,
        featurelist_get_historical_features,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_not_saved(
    historical_features_service,
    production_ready_feature,
    mock_get_historical_features,
    output_table_details,
):
    """
    Test compute_historical_features when feature list is not saved
    """
    feature_list = FeatureList(
        [Feature(**production_ready_feature.model_dump(by_alias=True))], name="mylist"
    )
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
        feature_list_id=feature_list.id,
        feature_clusters=feature_list._get_feature_clusters(),
    )
    training_events = pd.DataFrame({"cust_id": [1], "POINT_IN_TIME": ["2022-01-01"]})

    await historical_features_service.compute(
        training_events,
        featurelist_get_historical_features,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_deployed(
    historical_features_service,
    deployed_feature_list,
    mock_get_historical_features,
    output_table_details,
):
    """
    Test compute_historical_features when feature list is deployed
    """
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
        feature_list_id=deployed_feature_list.id,
        feature_clusters=deployed_feature_list.feature_clusters,
    )
    training_events = pd.DataFrame({"cust_id": [1], "POINT_IN_TIME": ["2022-01-01"]})

    await historical_features_service.compute(
        training_events,
        featurelist_get_historical_features,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once


@pytest.mark.asyncio
async def test_get_historical_features__feature_clusters_not_set(
    historical_features_service,
    deployed_feature_list,
    mock_get_historical_features,
    output_table_details,
):
    """
    Test compute_historical_features when feature list is deployed and feature clusters are not set
    """
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
        feature_list_id=deployed_feature_list.id,
    )
    training_events = pd.DataFrame({"cust_id": [1], "POINT_IN_TIME": ["2022-01-01"]})

    await historical_features_service.compute(
        training_events,
        featurelist_get_historical_features,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once


@pytest.mark.asyncio
async def test_get_historical_features__missing_point_in_time(
    mock_snowflake_feature,
    mock_snowflake_session,
    output_table_details,
    tile_cache_service,
    warehouse_table_service,
    cron_helper,
    snowflake_feature_store,
):
    """Test validation of missing point in time for historical features"""
    observation_set = pd.DataFrame({
        "cust_id": ["C1", "C2", "C3"],
    })
    with pytest.raises(exception.MissingPointInTimeColumnError) as exc_info:
        await get_historical_features(
            session=mock_snowflake_session,
            tile_cache_service=tile_cache_service,
            warehouse_table_service=warehouse_table_service,
            cron_helper=cron_helper,
            graph=mock_snowflake_feature.graph,
            nodes=[mock_snowflake_feature.node],
            observation_set=observation_set,
            feature_store=snowflake_feature_store,
            output_table_details=output_table_details,
        )
    assert str(exc_info.value) == "POINT_IN_TIME column is required"


@freeze_time("2022-05-01")
@pytest.mark.parametrize("point_in_time_is_datetime_dtype", [True, False])
@pytest.mark.asyncio
async def test_get_historical_features__too_recent_point_in_time(
    mock_snowflake_feature,
    mock_snowflake_session,
    point_in_time_is_datetime_dtype,
    output_table_details,
    tile_cache_service,
    warehouse_table_service,
    cron_helper,
    snowflake_feature_store,
):
    """Test validation of too recent point in time for historical features"""
    point_in_time_vals = ["2022-04-15", "2022-04-30"]
    if point_in_time_is_datetime_dtype:
        point_in_time_vals = pd.to_datetime(point_in_time_vals)
    observation_set = pd.DataFrame({
        "POINT_IN_TIME": point_in_time_vals,
        "cust_id": ["C1", "C2"],
    })
    with pytest.raises(exception.TooRecentPointInTimeError) as exc_info:
        await get_historical_features(
            session=mock_snowflake_session,
            tile_cache_service=tile_cache_service,
            warehouse_table_service=warehouse_table_service,
            cron_helper=cron_helper,
            graph=mock_snowflake_feature.graph,
            nodes=[mock_snowflake_feature.node],
            observation_set=observation_set,
            feature_store=snowflake_feature_store,
            output_table_details=output_table_details,
        )
    assert str(exc_info.value) == (
        "The latest point in time (2022-04-30 00:00:00) should not be more recent than 48 hours "
        "from now"
    )


@pytest.mark.asyncio
async def test_get_historical_features__point_in_time_dtype_conversion(
    float_feature,
    mock_snowflake_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
    tile_cache_service,
    warehouse_table_service,
    cron_helper,
    snowflake_feature_store,
):
    """
    Test that if point in time column is provided as string, it is converted to datetime before
    being registered as a temp table in session
    """
    # Input POINT_IN_TIME is string
    df_request = pd.DataFrame({
        "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
        "cust_id": ["C1", "C2"],
    })
    assert df_request.dtypes["POINT_IN_TIME"] == "object"

    mock_snowflake_session.generate_session_unique_id.return_value = "1"
    await get_historical_features(
        session=mock_snowflake_session,
        tile_cache_service=tile_cache_service,
        warehouse_table_service=warehouse_table_service,
        cron_helper=cron_helper,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        observation_set=df_request,
        feature_store=snowflake_feature_store,
        output_table_details=output_table_details,
    )

    # Check POINT_IN_TIME is converted to datetime
    mock_snowflake_session.register_table.assert_awaited_once()
    args, _ = mock_snowflake_session.register_table.await_args_list[0]
    df_observation_set_registered = args[1]
    assert df_observation_set_registered.dtypes["POINT_IN_TIME"] == "datetime64[ns]"

    mocked_compute_tiles_on_demand.assert_called_once()


@pytest.mark.asyncio
async def test_get_historical_features__intermediate_tables_dropped(
    float_feature,
    mock_snowflake_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
    tile_cache_service,
    warehouse_table_service,
    cron_helper,
    snowflake_feature_store,
):
    """
    Test intermediate tables are dropped after get historical features
    """
    _ = mocked_compute_tiles_on_demand
    df_request = pd.DataFrame({
        "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
        "cust_id": ["C1", "C2"],
    })
    mock_snowflake_session.generate_session_unique_id.return_value = "1"
    with patch.object(
        cron_helper, "register_job_schedule_tables"
    ) as mock_register_job_schedule_tables:
        mock_register_job_schedule_tables.return_value = JobScheduleTableSet(
            tables=[
                JobScheduleTable(
                    table_name="cron_schedule_1",
                    cron_feature_job_setting=CronFeatureJobSetting(crontab="0 0 * * *"),
                )
            ]
        )
        await get_historical_features(
            session=mock_snowflake_session,
            tile_cache_service=tile_cache_service,
            warehouse_table_service=warehouse_table_service,
            cron_helper=cron_helper,
            graph=float_feature.graph,
            nodes=[float_feature.node],
            observation_set=df_request,
            feature_store=snowflake_feature_store,
            output_table_details=output_table_details,
        )
    assert mock_snowflake_session.drop_table.call_args_list == [
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
        ),
        call(
            table_name="REQUEST_TABLE_1",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
        call(
            table_name="cron_schedule_1",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
    ]


@pytest.mark.asyncio
async def test_get_historical_features__tile_tables_dropped(
    float_feature,
    mock_snowflake_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
    tile_cache_service,
    warehouse_table_service,
    cron_helper,
    snowflake_feature_store,
):
    """
    Test temporary tile tables are dropped after get historical features
    """
    tile_compute_result = OnDemandTileComputeResult(
        tile_compute_metrics=TileComputeMetrics(),
        on_demand_tile_tables=[
            OnDemandTileTable(
                tile_table_id="TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
                on_demand_table_name="__temp_tile_table_1",
            ),
            OnDemandTileTable(
                tile_table_id="tile_id_1b",
                on_demand_table_name="__temp_tile_table_1",
            ),
            OnDemandTileTable(
                tile_table_id="tile_id_1c",
                on_demand_table_name="__temp_tile_table_1",
            ),
        ],
    )
    mocked_compute_tiles_on_demand.return_value = tile_compute_result
    df_request = pd.DataFrame({
        "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
        "cust_id": ["C1", "C2"],
    })
    mock_snowflake_session.generate_session_unique_id.return_value = "1"

    async def mock_func(*args, **kwargs):
        _ = args
        _ = kwargs
        tile_table_names = {
            table.on_demand_table_name for table in tile_compute_result.on_demand_tile_tables
        }
        for table_name in tile_table_names:
            yield WarehouseTableModel(
                location=TabularSource(
                    feature_store_id=snowflake_feature_store.id,
                    table_details=TableDetails(
                        table_name=table_name,
                        schema_name=mock_snowflake_session.schema_name,
                        database_name=mock_snowflake_session.database_name,
                    ),
                )
            )

    with patch(
        "featurebyte.service.warehouse_table_service.WarehouseTableService.list_warehouse_tables_by_tag",
        side_effect=mock_func,
    ) as patched_list_warehouse_tables_by_tag:
        await get_historical_features(
            session=mock_snowflake_session,
            tile_cache_service=tile_cache_service,
            warehouse_table_service=warehouse_table_service,
            cron_helper=cron_helper,
            graph=float_feature.graph,
            nodes=[float_feature.node],
            observation_set=df_request,
            feature_store=snowflake_feature_store,
            output_table_details=output_table_details,
        )

    assert patched_list_warehouse_tables_by_tag.call_args == call(
        "historical_features_SOME_HISTORICAL_FEATURE_TABLE"
    )
    assert mock_snowflake_session.drop_table.call_args_list == [
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
        ),
        call(
            table_name="REQUEST_TABLE_1",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
        call(
            table_name="__temp_tile_table_1",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
    ]
