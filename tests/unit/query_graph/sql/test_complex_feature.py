"""
Test sql generation for complex features
"""
from tests.util.helper import assert_equal_with_expected_fixture


def test_combined_simple_aggregate_and_window_aggregate(
    snowflake_event_view_with_entity_and_feature_job,
    snowflake_item_view_with_entity,
    update_fixtures,
):
    """
    Test combining simple aggregate and window aggregate
    """
    event_view = snowflake_event_view_with_entity_and_feature_job
    item_view = snowflake_item_view_with_entity
    item_feature = item_view.groupby("event_id_col").aggregate(
        method="count", feature_name="my_item_feature"
    )
    event_view = event_view.add_feature("added_feature", item_feature, "col_int")

    window_feature = event_view.groupby("cust_id").aggregate_over(
        value_column="added_feature",
        method="sum",
        windows=["7d"],
        feature_names=["added_feature_sum_7d"],
    )["added_feature_sum_7d"]

    feature = window_feature + item_feature
    feature.name = "combined_feature"

    assert_equal_with_expected_fixture(
        feature.sql,
        "tests/fixtures/expected_feature_sql_combined_simple_aggregate_and_window_aggregate.sql",
        update_fixtures,
    )
