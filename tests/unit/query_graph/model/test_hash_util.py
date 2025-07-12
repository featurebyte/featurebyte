"""
Test hash utilities for query graph nodes.
"""

from featurebyte import AggFunc, FeatureJobSetting


def test_feature_hash(snowflake_event_table_with_partition_column):
    """
    Test feature hash utility function.
    """
    event_table = snowflake_event_table_with_partition_column
    event_table.update_default_feature_job_setting(
        FeatureJobSetting(
            blind_spot="165s",
            period="60m",
            offset="135s",
        )
    )
    event_view = event_table.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method=AggFunc.SUM,
        windows=["1w"],
        feature_names=["amount_sum_1w"],
    )["amount_sum_1w"]
    feature.save()
    assert feature.cached_model.definition_hash == "1b6da274e13bf0b33633ee36e17c611f337224fd"
