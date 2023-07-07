"""
Test cases for the HistoricalFeaturesService
"""
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte import Feature, FeatureList, SourceType, exception
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.service.historical_features import get_historical_features
from featurebyte.session.base import BaseSession


@pytest.fixture
def mock_get_historical_features():
    """
    Mock the core compute_historical_features function that the service calls
    """
    with patch(
        "featurebyte.service.historical_features.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        with patch(
            "featurebyte.service.historical_features.get_historical_features"
        ) as mock_get_historical_features:
            mock_get_feature_store_session.return_value = Mock()
            yield mock_get_historical_features


@pytest.fixture(name="output_table_details")
def output_table_details_fixture():
    """Fixture for a TableDetails for the output location"""
    return TableDetails(table_name="SOME_HISTORICAL_FEATURE_TABLE")


@pytest.fixture(name="mocked_session")
def mocked_session_fixture():
    """Fixture for a mocked session object"""
    with patch("featurebyte.service.session_manager.SessionManager") as session_manager_cls:
        session_manager = AsyncMock(name="MockedSessionManager")
        mocked_session = Mock(
            name="MockedSession",
            spec=BaseSession,
            database_name="sf_database",
            schema_name="sf_schema",
            source_type=SourceType.SNOWFLAKE,
        )
        session_manager_cls.return_value = session_manager
        yield mocked_session


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_not_deployed(
    historical_features_service,
    production_ready_feature_list,
    get_credential,
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

    await historical_features_service.compute_historical_features(
        training_events,
        featurelist_get_historical_features,
        get_credential,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once
    call_args = mock_get_historical_features.call_args
    assert call_args[1]["is_feature_list_deployed"] is False


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_not_saved(
    historical_features_service,
    production_ready_feature,
    get_credential,
    mock_get_historical_features,
    output_table_details,
):
    """
    Test compute_historical_features when feature list is not saved
    """
    feature_list = FeatureList(
        [Feature(**production_ready_feature.dict(by_alias=True))], name="mylist"
    )
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(
        feature_list_id=feature_list.id,
        feature_clusters=feature_list._get_feature_clusters(),
    )
    training_events = pd.DataFrame({"cust_id": [1], "POINT_IN_TIME": ["2022-01-01"]})

    await historical_features_service.compute_historical_features(
        training_events,
        featurelist_get_historical_features,
        get_credential,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once
    call_args = mock_get_historical_features.call_args
    assert call_args[1]["is_feature_list_deployed"] is False


@pytest.mark.asyncio
async def test_get_historical_features__feature_list_deployed(
    historical_features_service,
    deployed_feature_list,
    get_credential,
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

    await historical_features_service.compute_historical_features(
        training_events,
        featurelist_get_historical_features,
        get_credential,
        output_table_details=output_table_details,
    )
    assert mock_get_historical_features.assert_called_once
    call_args = mock_get_historical_features.call_args
    assert call_args[1]["is_feature_list_deployed"] is True


@pytest.mark.asyncio
async def test_get_historical_features__missing_point_in_time(
    mock_snowflake_feature,
    mocked_session,
    output_table_details,
    tile_cache_service,
    snowflake_feature_store,
):
    """Test validation of missing point in time for historical features"""
    observation_set = pd.DataFrame(
        {
            "cust_id": ["C1", "C2", "C3"],
        }
    )
    with pytest.raises(exception.MissingPointInTimeColumnError) as exc_info:
        await get_historical_features(
            session=mocked_session,
            tile_cache_service=tile_cache_service,
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
    mocked_session,
    point_in_time_is_datetime_dtype,
    output_table_details,
    tile_cache_service,
    snowflake_feature_store,
):
    """Test validation of too recent point in time for historical features"""
    point_in_time_vals = ["2022-04-15", "2022-04-30"]
    if point_in_time_is_datetime_dtype:
        point_in_time_vals = pd.to_datetime(point_in_time_vals)
    observation_set = pd.DataFrame(
        {
            "POINT_IN_TIME": point_in_time_vals,
            "cust_id": ["C1", "C2"],
        }
    )
    with pytest.raises(exception.TooRecentPointInTimeError) as exc_info:
        await get_historical_features(
            session=mocked_session,
            tile_cache_service=tile_cache_service,
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
    mocked_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
    tile_cache_service,
    snowflake_feature_store,
):
    """
    Test that if point in time column is provided as string, it is converted to datetime before
    being registered as a temp table in session
    """
    # Input POINT_IN_TIME is string
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    assert df_request.dtypes["POINT_IN_TIME"] == "object"

    mocked_session.generate_session_unique_id.return_value = "1"
    await get_historical_features(
        session=mocked_session,
        tile_cache_service=tile_cache_service,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        observation_set=df_request,
        feature_store=snowflake_feature_store,
        output_table_details=output_table_details,
    )

    # Check POINT_IN_TIME is converted to datetime
    mocked_session.register_table.assert_awaited_once()
    args, _ = mocked_session.register_table.await_args_list[0]
    df_observation_set_registered = args[1]
    assert df_observation_set_registered.dtypes["POINT_IN_TIME"] == "datetime64[ns]"

    mocked_compute_tiles_on_demand.assert_called_once()


@pytest.mark.asyncio
async def test_get_historical_features__skip_tile_cache_if_deployed(
    float_feature,
    mocked_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
    tile_cache_service,
    snowflake_feature_store,
):
    """
    Test that for with is_feature_list_deployed=True on demand tile computation is skipped
    """
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    mocked_session.generate_session_unique_id.return_value = "1"
    await get_historical_features(
        session=mocked_session,
        tile_cache_service=tile_cache_service,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        observation_set=df_request,
        feature_store=snowflake_feature_store,
        is_feature_list_deployed=True,
        output_table_details=output_table_details,
    )
    mocked_compute_tiles_on_demand.assert_not_called()


@pytest.mark.asyncio
async def test_get_historical_features__tile_cache_multiple_batches(
    float_feature,
    agg_per_category_feature,
    tile_cache_service,
    output_table_details,
    mocked_session,
    mocked_compute_tiles_on_demand,
    snowflake_feature_store,
):
    """
    Test that nodes for tile cache are batched correctly
    """
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    mocked_session.generate_session_unique_id.return_value = "1"

    complex_feature = float_feature * agg_per_category_feature.cd.entropy()
    graph, node = complex_feature.extract_pruned_graph_and_node()
    nodes = [graph.get_node_by_name("groupby_1"), graph.get_node_by_name("groupby_2")]

    with patch("featurebyte.service.historical_features.NUM_FEATURES_PER_QUERY", 1):
        _ = await get_historical_features(
            session=mocked_session,
            tile_cache_service=tile_cache_service,
            graph=graph,
            nodes=nodes,
            observation_set=df_request,
            feature_store=snowflake_feature_store,
            output_table_details=output_table_details,
        )

    assert len(mocked_compute_tiles_on_demand.call_args_list) == 2

    nodes = []
    for call_args in mocked_compute_tiles_on_demand.call_args_list:
        _, kwargs = call_args
        current_nodes = kwargs["nodes"]
        nodes.extend([node.name for node in current_nodes])

    expected_nodes = ["groupby_2", "groupby_1"]
    assert nodes == expected_nodes
