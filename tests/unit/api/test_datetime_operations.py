"""
Tests for datetime operations in the SDK
"""

import pytest

from featurebyte.core.datetime import to_timestamp_from_epoch
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.util.helper import assert_equal_with_expected_fixture


def test_timestamp_diff_with_schema(snowflake_time_series_view_with_entity, update_fixtures):
    """
    Test difference between timestamps with TimestampSchema
    """
    view = snowflake_time_series_view_with_entity
    view["new_col"] = view["date"] - view["another_timestamp_col"]
    preview_sql = view.preview_sql()
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/api/test_datetime_operations/timestamp_diff_with_schema.sql",
        update_fixtures,
    )


def test_to_timestamp_from_epoch(snowflake_event_view_with_entity, update_fixtures):
    """
    Test to_timestamp_from_epoch
    """
    view = snowflake_event_view_with_entity
    view["converted_timestamp"] = to_timestamp_from_epoch(view["col_int"])
    view["converted_timestamp_hour"] = view["converted_timestamp"].dt.hour
    preview_sql = view.preview_sql()
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/api/test_datetime_operations/to_timestamp_from_epoch.sql",
        update_fixtures,
    )
    feature = view.groupby("cust_id").aggregate_over(
        value_column="converted_timestamp_hour",
        method="max",
        windows=["2d"],
        feature_names=["max_hour_2d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1h",
            period="1h",
            offset="0s",
        ),
    )["max_hour_2d"]
    feature.save()


def test_to_timestamp_from_epoch__non_numeric(snowflake_event_view_with_entity, update_fixtures):
    """
    Test to_timestamp_from_epoch
    """
    view = snowflake_event_view_with_entity
    with pytest.raises(ValueError) as exc_info:
        view["converted_timestamp"] = to_timestamp_from_epoch(view["col_text"])
    assert (
        str(exc_info.value) == "to_timestamp_from_epoch requires input to be numeric, got VARCHAR"
    )
