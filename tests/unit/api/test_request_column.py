"""
RequestColumn unit tests
"""
import pytest

from featurebyte.api.request_column import RequestColumn, point_in_time


@pytest.fixture(name="latest_event_timestamp_feature_fixture")
def latest_event_timestamp_feature_fixture(snowflake_event_view_with_entity):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=dict(blind_spot="1h", frequency="1h", time_modulo_frequency="30m"),
    )["latest_event_timestamp_90d"]
    return feature


def test_point_in_time_request_column():
    """
    Test point_in_time request column
    """
    assert isinstance(point_in_time, RequestColumn)
    node_dict = point_in_time.node.dict()
    assert node_dict == {
        "name": "request_column_1",
        "type": "request_column",
        "output_type": "series",
        "parameters": {"column_name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
    }


def test_point_in_time_minus_timestamp_feature(latest_event_timestamp_feature_fixture):
    """
    Test an on-demand feature involving point in time
    """
    time_since_last_event_feature = point_in_time - latest_event_timestamp_feature_fixture
    raise
