"""
Tests for featurebyte.query_graph.feature_historical.py
"""
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte import exception
from featurebyte.api.feature_store import FeatureStore
from featurebyte.enum import SourceType
from featurebyte.models.feature_store import SQLiteDetails
from featurebyte.query_graph.feature_historical import (
    get_historical_features,
    get_historical_features_sql,
    get_session_from_feature_objects,
)


@pytest.fixture(name="mocked_session")
def mocked_session_fixture():
    """Fixture for a mocked session object"""
    with patch("featurebyte.core.generic.SessionManager") as session_manager_cls:
        session_manager = MagicMock(name="MockedSessionManager")
        mocked_session = Mock(name="MockedSession", sf_schema="FEATUREBYTE")
        session_manager.__getitem__.return_value = mocked_session
        session_manager_cls.return_value = session_manager
        yield mocked_session


@pytest.fixture(name="mocked_tile_cache")
def mocked_tile_cache_fixture():
    """Fixture for a mocked SnowflakeTileCache object"""
    with patch(
        "featurebyte.query_graph.feature_historical.SnowflakeTileCache", autospec=True
    ) as mocked_cls:
        yield mocked_cls.return_value


@pytest.fixture(name="mock_sqlite_feature")
def mock_sqlite_feature_fixture():
    """Fixture for a mocked sqlite feature"""
    feature = Mock(name="MockFeature")
    feature_store = FeatureStore(type=SourceType.SQLITE, details=SQLiteDetails(filename="data.csv"))
    feature.tabular_source = (feature_store, Mock(name="MockTableDetails"))
    return feature


def test_get_historical_features__missing_point_in_time(mock_snowflake_feature):
    """Test validation of missing point in time for historical features"""
    training_events = pd.DataFrame(
        {
            "CUST_ID": ["C1", "C2", "C3"],
        }
    )
    with pytest.raises(exception.MissingPointInTimeColumnError) as exc_info:
        get_historical_features(
            feature_objects=[mock_snowflake_feature], training_events=training_events
        )
    assert str(exc_info.value) == "POINT_IN_TIME column is required"


@freeze_time("2022-05-01")
@pytest.mark.parametrize("point_in_time_is_datetime_dtype", [True, False])
def test_get_historical_features__too_recent_point_in_time(
    mock_snowflake_feature, point_in_time_is_datetime_dtype
):
    """Test validation of too recent point in time for historical features"""
    point_in_time_vals = ["2022-04-15", "2022-04-30"]
    if point_in_time_is_datetime_dtype:
        point_in_time_vals = pd.to_datetime(point_in_time_vals)
    training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": point_in_time_vals,
            "CUST_ID": ["C1", "C2"],
        }
    )
    with pytest.raises(exception.TooRecentPointInTimeError) as exc_info:
        get_historical_features(
            feature_objects=[mock_snowflake_feature], training_events=training_events
        )
    assert str(exc_info.value) == (
        "The latest point in time (2022-04-30 00:00:00) should not be more recent than 48 hours "
        "from now"
    )


def test_get_session__multiple_stores_not_supported(float_feature, mock_sqlite_feature):
    """Test multiple stores not supported"""
    with pytest.raises(NotImplementedError) as exc_info:
        get_session_from_feature_objects([float_feature, mock_sqlite_feature])
    assert str(exc_info.value) == "Historical features request using multiple stores not supported"


def test_get_historical_features__point_in_time_dtype_conversion(
    float_feature,
    config,
    mocked_session,
    mocked_tile_cache,
):
    """
    Test that if point in time column is provided as string, it is converted to datetime before
    being registered as a temp table in session
    """
    feature_objects = [float_feature]

    # Input POINT_IN_TIME is string
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "CUST_ID": ["C1", "C2"],
        }
    )
    assert df_request.dtypes["POINT_IN_TIME"] == "object"

    _ = get_historical_features(
        feature_objects=feature_objects,
        training_events=df_request,
        credentials=config.credentials,
    )

    # Check POINT_IN_TIME is converted to datetime
    mocked_session.register_temp_table.assert_called_once()
    args, _ = mocked_session.register_temp_table.call_args_list[0]
    df_training_events_registered = args[1]
    assert df_training_events_registered.dtypes["POINT_IN_TIME"] == "datetime64[ns]"

    mocked_tile_cache.compute_tiles_on_demand.assert_called_once()


def test_get_historical_feature_sql(float_feature):
    """Test SQL code generated for historical features is expected"""
    feature_objects = [float_feature]
    request_table_columns = ["POINT_IN_TIME", "CUST_ID", "A", "B", "C"]
    sql = get_historical_features_sql(
        feature_objects=feature_objects, request_table_columns=request_table_columns
    )

    update_fixtures = False
    if update_fixtures:
        with open(
            "tests/fixtures/expected_historical_requests.sql", "w", encoding="utf-8"
        ) as f_handle:
            f_handle.write(sql)
            raise AssertionError("Fixture updated, please set update_fixture to False")
    with open("tests/fixtures/expected_historical_requests.sql", encoding="utf-8") as f_handle:
        expected_feature_sql = f_handle.read()

    assert sql.strip() == expected_feature_sql.strip()
