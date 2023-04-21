"""
Integration tests for SnowflakeTileCache
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte.enum import InternalName
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.tile.tile_cache import OnDemandTileComputeRequest, TileCache


@pytest.fixture(name="feature_for_tile_cache_tests")
def feature_for_tile_cache_tests_fixture(event_table, groupby_category):
    """Fixture for a feature used for tile cache test

    Should not be shared with other tests because of side effects after running on-demand tiles
    computation, get_historical_features(), etc.
    """
    event_view = event_table.get_view()
    feature_group = event_view.groupby("ÜSER ID", category=groupby_category).aggregate_over(
        method="count",
        windows=["48h"],
        feature_names=["SESSION_COUNT_48h"],
        feature_job_setting={
            "blind_spot": "45m",
            "frequency": "1h",
            "time_modulo_frequency": "30m",
        },
    )
    yield feature_group["SESSION_COUNT_48h"]


async def check_entity_table_sql_and_tile_compute_sql(
    session,
    request: OnDemandTileComputeRequest,
    entity_column,
    expected_entities,
):
    """Test SQLs for entity table and tiles computation produce correct results"""
    df_entity = await session.execute_query(request.tracker_sql)
    assert df_entity[entity_column].isin(expected_entities).all()

    df_tiles = await session.execute_query(request.tile_compute_sql)
    assert df_tiles[entity_column].isin(expected_entities).all()


async def check_temp_tables_cleaned_up(session):
    """Check that temp tables are properly cleaned up"""
    if session.source_type == "snowflake":
        df_tables = await session.execute_query("SHOW TABLES")
        temp_table_names = df_tables[df_tables["kind"] == "TEMPORARY"]["name"].tolist()
    elif session.source_type == "spark":
        df_tables = await session.execute_query("SHOW VIEWS")
        temp_table_names = df_tables[df_tables["isTemporary"] == "true"]["viewName"].tolist()
    else:
        raise NotImplementedError()
    temp_table_names = [name.upper() for name in temp_table_names]
    assert InternalName.TILE_CACHE_WORKING_TABLE.value not in temp_table_names


async def invoke_tile_manager_and_check_tracker_table(session, tile_cache, requests):
    """
    Call tile_cache.invoke_tile_manager() and check that the tracker table is updated correctly
    """
    await tile_cache.invoke_tile_manager(requests)

    for request in requests:
        tracker_table_name = f"{request.aggregation_id}_entity_tracker"
        df_entity_tracker = await session.execute_query(f"SELECT * FROM {tracker_table_name}")

        # The üser id column should be the primary key (unique) of the tracker table
        assert (df_entity_tracker["üser id".upper()].value_counts(dropna=False) == 1).all()


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.parametrize("groupby_category", [None, "PRODUCT_ACTION"])
@pytest.mark.asyncio
async def test_tile_cache(session, feature_for_tile_cache_tests, groupby_category):
    """Test TileCache performs caching properly"""
    feature = feature_for_tile_cache_tests
    tile_cache = TileCache(session)
    _ = groupby_category

    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00"] * 5),
            "üser id": [1, 2, 3, 4, np.nan],
        }
    )

    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    await session.register_table(request_table_name, df_training_events)

    # No cache existed before for this feature. Check that one tile table needs to be computed
    requests = await tile_cache.get_required_computation(
        request_id=request_id,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_name=request_table_name,
    )
    assert len(requests) == 1
    await check_entity_table_sql_and_tile_compute_sql(
        session,
        requests[0],
        "ÜSER ID",
        [1, 2, 3, 4, np.nan],
    )
    await invoke_tile_manager_and_check_tracker_table(session, tile_cache, requests)
    await tile_cache.cleanup_temp_tables()
    await check_temp_tables_cleaned_up(session)

    # Cache now exists. No additional compute required for the same request table
    requests = await tile_cache.get_required_computation(
        request_id=request_id,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_name=request_table_name,
    )
    assert len(requests) == 0

    # Check using training events with outdated entities (user 3, 4, 5)
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(
                ["2001-01-02 10:00:00"] * 2 + ["2001-01-03 10:00:00"] * 3
            ),
            "üser id": [1, 2, 3, 4, np.nan],
        }
    )
    await session.register_table(request_table_name, df_training_events)
    requests = await tile_cache.get_required_computation(
        request_id=request_id,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_name=request_table_name,
    )
    assert len(requests) == 1
    await check_entity_table_sql_and_tile_compute_sql(
        session,
        requests[0],
        "ÜSER ID",
        [3, 4, np.nan],
    )
    await invoke_tile_manager_and_check_tracker_table(session, tile_cache, requests)
    await tile_cache.cleanup_temp_tables()
    await check_temp_tables_cleaned_up(session)

    # Cache now exists. No additional compute required for the same request table
    requests = await tile_cache.get_required_computation(
        request_id=request_id,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_name=request_table_name,
    )
    assert len(requests) == 0

    # Check using training events with new entities
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00"] * 2),
            "üser id": [6, 7],
        }
    )

    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    await session.register_table(request_table_name, df_training_events)
    requests = await tile_cache.get_required_computation(
        request_id=request_id,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_name=request_table_name,
    )
    assert len(requests) == 1
    await check_entity_table_sql_and_tile_compute_sql(
        session,
        requests[0],
        "ÜSER ID",
        [6, 7],
    )
    await invoke_tile_manager_and_check_tracker_table(session, tile_cache, requests)
    await tile_cache.cleanup_temp_tables()
    await check_temp_tables_cleaned_up(session)
