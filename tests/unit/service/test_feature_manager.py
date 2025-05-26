"""
Test FeatureManagerService
"""

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.tile_registry import BackfillMetadata, LastRunMetadata, TileUpdate
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(autouse=True)
def mock_schedule_online_store_current_ts():
    """
    Fixture to mock datetime.now() to a fixed time in TileScheduleOnlineStore
    """
    with patch("featurebyte.sql.tile_schedule_online_store.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2022, 5, 15, 10, 0, 5)
        yield


@pytest.fixture
def feature_manager_service(app_container):
    """
    Fixture for a FeatureManagerService
    """
    return app_container.feature_manager_service


@pytest.fixture
def feature_2h(snowflake_event_view_with_entity):
    """
    Fixture for a feature with 2h derivation window
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=None,
        method="count",
        windows=["2h"],
        feature_names=["COUNT_2h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m",
            frequency="1h",
            time_modulo_frequency="15m",
        ),
    )["COUNT_2h"]
    feature.save()
    return feature


@pytest.fixture
def feature_4h(snowflake_event_view_with_entity):
    """
    Fixture for a feature with 4h derivation window
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=None,
        method="count",
        windows=["4h"],
        feature_names=["COUNT_4h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m",
            frequency="1h",
            time_modulo_frequency="15m",
        ),
    )["COUNT_4h"]
    feature.save()
    return feature


@pytest.fixture
def feature_2h_with_offset(snowflake_event_view_with_entity):
    """
    Fixture for a feature with 2h derivation window and an offset
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=None,
        method="count",
        windows=["2h"],
        feature_names=["COUNT_2h_OFFSET_4h"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m",
            frequency="1h",
            time_modulo_frequency="15m",
        ),
        offset="4h",
    )["COUNT_2h_OFFSET_4h"]
    feature.save()
    return feature


def get_online_feature_spec(feature_model):
    """
    Helper function to get an OnlineFeatureSpec from a feature
    """
    extended_feature_model = ExtendedFeatureModel(**feature_model.model_dump(by_alias=True))
    return extended_feature_model


async def deployment_enable_setup(app_container, feature_model):
    """
    Run necessary setup as part of the full online enabling process for a feature. This is assumed
    to be done when FeatureManager's online_enable() or online_disable() is called
    """
    deployed_tile_table_manager_service = app_container.deployed_tile_table_manager_service
    await deployed_tile_table_manager_service.handle_online_enabled_features([feature_model])


async def get_deployed_tile_table_model(app_container, feature_model):
    """
    Helper function to get a deployed tile table model for a feature
    """
    service = app_container.deployed_tile_table_service
    async for doc in service.list_deployed_tile_tables_by_aggregation_ids(
        set(feature_model.aggregation_ids)
    ):
        return doc


@pytest.mark.asyncio
async def test_enable_new_feature(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    update_fixtures,
):
    """
    Test enabling one feature without any existing backfill metadata
    """
    feature_model = feature_2h.cached_model

    deployed_tile_table = await get_deployed_tile_table_model(app_container, feature_model)
    assert deployed_tile_table is None

    # Online enable a feature
    await deployment_enable_setup(app_container, feature_model)
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(app_container, feature_model)
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Check executed queries
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_new_feature.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_feature_with_existing_backfill_metadata(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    feature_4h,
    update_fixtures,
):
    """
    Test enabling two features with shared tile
    """
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table is None

    # Enable feature_2h. This sets up the backfill metadata of the deployed tile table model
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Enable feature_4h. This should use the existing tile registry metadata to determine what
    # should be backfilled
    mock_snowflake_session.list_table_schema.return_value = {
        "tile_col_1": "some_info",
        "tile_col_2": "some_info",
    }
    mock_snowflake_session.reset_mock()
    mock_snowflake_session.table_exists.side_effect = lambda _: True
    await deployment_enable_setup(app_container, feature_4h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_4h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 4, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Check executed queries
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_with_backfill_metadata.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_feature_with_required_tiles_available(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    feature_4h,
    update_fixtures,
):
    """
    Test enabling a feature when its required tiles are already generated
    """
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table is None

    # Enable feature_4h. This sets up the tile registry metadata
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_4h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_4h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 4, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Enable feature_2h. No tiles need to be generated again because feature_4h has already
    # generated the required tiles
    mock_snowflake_session.reset_mock()
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata remains the same
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 4, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Check executed queries. There shouldn't be any tile compute queries but only the feature
    # compute query using existing tiles.
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_with_required_tiles_available.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_second_feature_later_date(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    feature_4h,
    update_fixtures,
):
    """
    Test enabling a feature with a later schedule date. This requires two queries for the tile
    backfill.
    """
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table is None

    # Enable feature_2h. This sets up the tile registry metadata
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Enable feature_4h at a later schedule time
    mock_snowflake_session.list_table_schema.return_value = {
        "tile_col_1": "some_info",
        "tile_col_2": "some_info",
    }
    mock_snowflake_session.reset_mock()
    mock_snowflake_session.table_exists.side_effect = lambda _: True
    await deployment_enable_setup(app_container, feature_4h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_4h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 11:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 5, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 9, 45), index=459058
    )

    # Check executed queries. This requires two tile compute queries for the backfill.
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_second_feature_later_date.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_second_feature_much_later_date(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    feature_4h,
    update_fixtures,
):
    """
    Test enabling a feature with a much later schedule date. This requires computation of recent
    tiles, and should not update backfill metadata start date.
    """
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table is None

    # Enable feature_2h. This sets up the tile registry metadata
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Enable feature_4h at a much later schedule time when last_run_metadata_offline remains the
    # same. This simulates the scenario where feature_2h is no longer enabled so no scheduled tile
    # job has been running.
    mock_snowflake_session.list_table_schema.return_value = {
        "tile_col_1": "some_info",
        "tile_col_2": "some_info",
    }
    mock_snowflake_session.reset_mock()
    mock_snowflake_session.table_exists.side_effect = lambda _: True
    await deployment_enable_setup(app_container, feature_4h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_4h.cached_model),
        schedule_time=pd.Timestamp("2022-06-15 11:00:00"),
    )

    # Check tile registry metadata. backfill_metadata start date should remain the same as before.
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_4h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 6, 15, 9, 45), index=459802
    )

    # Check executed queries. This requires two tile compute queries for the backfill.
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_second_feature_much_later_date.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_with_backfill_metadata_but_not_last_run_metadata(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h,
    update_fixtures,
):
    """
    Test enabling a feature when backfill metadata is available but last run metadata is not (less
    likely case).
    """
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table is None

    # Enable feature_2h. This sets up the tile registry metadata
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Online disable the feature
    await feature_manager_service.online_disable(
        session=mock_snowflake_session,
        feature=feature_2h.cached_model,
    )

    # Simulate missing last run metadata by unsetting it
    update_schema = TileUpdate(
        last_run_metadata_offline=None,
        backfill_metadata=deployed_tile_table.backfill_metadata,
    )
    await app_container.deployed_tile_table_service.update_document(
        document_id=deployed_tile_table.id,
        data=update_schema,
        exclude_none=False,
        document=deployed_tile_table,
    )
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table.backfill_metadata is not None
    assert deployed_tile_table.last_run_metadata_offline is None

    # Enable feature_2h at a much later schedule time. This requires tiles computation.
    mock_snowflake_session.list_table_schema.return_value = {
        "tile_col_1": "some_info",
        "tile_col_2": "some_info",
    }
    mock_snowflake_session.reset_mock()
    mock_snowflake_session.table_exists.side_effect = lambda _: True
    await deployment_enable_setup(app_container, feature_2h.cached_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_2h.cached_model),
        schedule_time=pd.Timestamp("2022-06-15 11:00:00"),
    )

    # Check tile registry metadata. backfill_metadata start date should remain the same as before.
    # last_run_metadata_offline should be set again.
    deployed_tile_table = await get_deployed_tile_table_model(
        app_container, feature_2h.cached_model
    )
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 6, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 6, 15, 9, 45), index=459802
    )

    # Check executed queries. This requires two tile compute queries for the backfill.
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_without_last_run_metadata.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_enable_feature_with_offset(
    app_container,
    feature_manager_service,
    mock_snowflake_session,
    feature_2h_with_offset,
    update_fixtures,
):
    """
    Test enabling one feature with an offset without any existing backfill metadata
    """
    feature_model = feature_2h_with_offset.cached_model

    deployed_tile_table = await get_deployed_tile_table_model(app_container, feature_model)
    assert deployed_tile_table is None

    mock_snowflake_session.table_exists.side_effect = lambda _: False
    await deployment_enable_setup(app_container, feature_model)
    await feature_manager_service.online_enable(
        session=mock_snowflake_session,
        feature_spec=get_online_feature_spec(feature_model),
        schedule_time=pd.Timestamp("2022-05-15 10:00:00"),
    )

    # Check tile registry metadata updated
    deployed_tile_table = await get_deployed_tile_table_model(app_container, feature_model)
    assert deployed_tile_table.backfill_metadata == BackfillMetadata(
        start_date=datetime(2022, 5, 15, 2, 45)
    )
    assert deployed_tile_table.last_run_metadata_offline == LastRunMetadata(
        tile_end_date=datetime(2022, 5, 15, 8, 45), index=459057
    )

    # Check executed queries
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_manager/enable_new_feature_offset.sql",
        update_fixtures,
    )
