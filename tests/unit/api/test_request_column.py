"""
RequestColumn unit tests
"""
import pytest

from featurebyte import FeatureJobSetting
from featurebyte.api.feature import Feature
from featurebyte.api.request_column import RequestColumn
from featurebyte.enum import DBVarType
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="latest_event_timestamp_feature")
def latest_event_timestamp_feature_fixture(snowflake_event_view_with_entity):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1h", frequency="1h", time_modulo_frequency="30m"
        ),
    )["latest_event_timestamp_90d"]
    return feature


def test_point_in_time_request_column():
    """
    Test point_in_time request column
    """
    point_in_time = RequestColumn.point_in_time()
    assert isinstance(point_in_time, RequestColumn)
    node_dict = point_in_time.node.dict()
    assert node_dict == {
        "name": "request_column_1",
        "type": "request_column",
        "output_type": "series",
        "parameters": {"column_name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
    }


def test_point_in_time_minus_timestamp_feature(latest_event_timestamp_feature, update_fixtures):
    """
    Test an on-demand feature involving point in time
    """
    new_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    new_feature.name = "Time Since Last Event (days)"
    assert isinstance(new_feature, Feature)

    assert new_feature.entity_ids == latest_event_timestamp_feature.entity_ids
    assert new_feature.feature_store == latest_event_timestamp_feature.feature_store
    assert new_feature.tabular_source == latest_event_timestamp_feature.tabular_source

    new_feature.save()
    loaded_feature = Feature.get(new_feature.name)
    check_sdk_code_generation(
        loaded_feature,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_with_request_column.py",
        update_fixtures=update_fixtures,
        table_id=new_feature.table_ids[0],
    )


def test_request_column_non_point_in_time_blocked():
    """
    Test non-point-in-time request column is blocked
    """
    with pytest.raises(NotImplementedError) as exc:
        _ = RequestColumn.create_request_column("foo", DBVarType.FLOAT)
    assert "Currently only POINT_IN_TIME column is supported" in str(exc.value)
