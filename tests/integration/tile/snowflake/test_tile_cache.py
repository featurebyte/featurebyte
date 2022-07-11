"""
Integration tests for SnowflakeTileCache
"""
import pandas as pd
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME
from featurebyte.tile.tile_cache import SnowflakeOnDemandTileComputeRequest, SnowflakeTileCache


@pytest.fixture(name="feature_for_tile_cache_tests")
def feature_for_tile_cache_tests_fixture(event_data):
    """Fixture for a feature used for tile cache test

    Should not be shared with other tests because of side effects after running on-demand tiles
    computation, get_historical_features(), etc.
    """
    event_view = EventView.from_event_data(event_data)
    feature_group = event_view.groupby("USER_ID").aggregate(
        "SESSION_ID",
        "count",
        windows=["48h"],
        blind_spot="30m",
        frequency="1h",
        time_modulo_frequency="30m",
        feature_names=["SESSION_COUNT_48h"],
    )
    yield feature_group["SESSION_COUNT_48h"]


def check_entity_table_sql_and_tile_compute_sql(
    session,
    request: SnowflakeOnDemandTileComputeRequest,
    entity_column,
    expected_entities,
):
    """Test SQLs for entity table and tiles computation produce correct results"""
    df_entity = session.execute_query(request.tracker_sql)
    assert df_entity[entity_column].isin(expected_entities).all()

    df_tiles = session.execute_query(request.tile_compute_sql)
    assert df_tiles[entity_column].isin(expected_entities).all()


def test_snowflake_tile_cache(snowflake_session, feature_for_tile_cache_tests):
    """Test SnowflakeTileCache performs caching properly"""
    feature = feature_for_tile_cache_tests
    tile_cache = SnowflakeTileCache(snowflake_session)
    feature_objects = [feature]

    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00"] * 5),
            "USER_ID": [1, 2, 3, 4, 5],
        }
    )
    snowflake_session.register_temp_table(REQUEST_TABLE_NAME, df_training_events)

    # No cache existed before for this feature. Check that one tile table need to be recomputed
    requests = tile_cache.get_required_computation(feature_objects)
    assert len(requests) == 1
    check_entity_table_sql_and_tile_compute_sql(
        snowflake_session,
        requests[0],
        "USER_ID",
        [1, 2, 3, 4, 5],
    )
    tile_cache.invoke_tile_manager(requests)

    # Cache now exists. No additional compute required for the same request table
    requests = tile_cache.get_required_computation(feature_objects)
    assert len(requests) == 0

    # Check using training events with outdated entities (user 3, 4, 5)
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(
                ["2001-01-02 10:00:00"] * 2 + ["2001-01-03 10:00:00"] * 3
            ),
            "USER_ID": [1, 2, 3, 4, 5],
        }
    )
    snowflake_session.register_temp_table(REQUEST_TABLE_NAME, df_training_events)
    requests = tile_cache.get_required_computation(feature_objects)
    assert len(requests) == 1
    check_entity_table_sql_and_tile_compute_sql(
        snowflake_session,
        requests[0],
        "USER_ID",
        [3, 4, 5],
    )
    tile_cache.invoke_tile_manager(requests)

    # Check using training events with new entities
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00"] * 2),
            "USER_ID": [6, 7],
        }
    )
    snowflake_session.register_temp_table(REQUEST_TABLE_NAME, df_training_events)
    requests = tile_cache.get_required_computation(feature_objects)
    assert len(requests) == 1
    check_entity_table_sql_and_tile_compute_sql(
        snowflake_session,
        requests[0],
        "USER_ID",
        [6, 7],
    )
    tile_cache.invoke_tile_manager(requests)
