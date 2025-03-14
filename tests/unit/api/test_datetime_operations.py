"""
Tests for datetime operations in the SDK
"""

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
